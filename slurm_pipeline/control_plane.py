import os
import uuid
import csv
import json
import time
import shutil
import logging
import datetime
from enum import Enum
from pathlib import Path
from collections import defaultdict, Counter

import yaml

from slurm_pipeline import config
from slurm_pipeline import slurm
from slurm_pipeline.config import UsageError
from slurm_pipeline.slurm import Status, SlurmException
from slurm_pipeline.cluster_utils.slack_notifications import SlackLoggingHandler
import slurm_pipeline.cluster_utils.slack_notifications as slack

logger = logging.getLogger(__name__)


class WorkPackage():

    Status = Enum('STATUS', 'PENDING FAILED SUCCEEDED')

    def __init__(self, params, cpus, time, partition=None):
        self.params = params
        self.cpus = cpus
        self.time = time
        self.partition = partition or self._determine_partition()
        self.qos = self._determine_qos()
        self.n_tries = 0
        self.name = 'TBD' #params['name']
        self.status = WorkPackage.Status.PENDING
        self.slurm_status = None
        self.error_msg = None
        self.stderr_log = None
        self.stdout_log = None
        self.job_id = None
        self.old_job_ids = []


    @staticmethod
    def init_failed(params, error_msg):
        wp = WorkPackage(params, cpus=None, time=None)
        # TODO: introduce INIT_FAILED (& ABORTED) status in order to differentiate between runtime and init failures when assessing if failure threshold has been reached
        wp.status = WorkPackage.Status.FAILED
        wp.error_msg = error_msg
        return wp


    def encode(self):
        return {
            'params': self.params,
            'cpus': self.cpus,
            'time': self.time,
            'partition': self.partition,
            'qos': self.qos,
            'name': self.name,
            'status': self.status.name,
            'slurm_status': self.slurm_status.name if self.slurm_status else None,
            'n_tries': self.n_tries,
            'job_id': self.job_id,
            'stdout_log': self.stdout_log,
            'stderr_log': self.stderr_log,
            'error_msg': self.error_msg,
            'old_job_ids': self.old_job_ids
        }


    def minutes(self):
        timedelta = slurm.parse_time(self.time)
        return round(timedelta.total_seconds() / 60)


    def to_json(self):
        return json.dumps(self.encode(), sort_keys=True, indent=4, ensure_ascii=False).encode('utf8')


    def _determine_partition(self):
        return 'standard' if self.cpus <= 16 else 'broadwell'


    def _determine_qos(self):
        if self.minutes() <= 24 * 60:
            return 'short'
        elif self.minutes() <= 24 * 60 * 7:
            return 'medium'
        else:
            return 'long'


class Scheduler():

    def __init__(self, job_config):
        self.job_config = job_config
        self.job_name = job_config['name']
        self.script = job_config['script']
        self.param_files = job_config['param_files']
        self.log_dir = job_config['log_dir']

        self.conda_env = job_config['properties']['conda_env']
        self.account = job_config['properties']['account']
        self.left_over = job_config['properties']['left_over']
        self.keep_work_dir = job_config['properties']['keep_work_dir']
        self.max_retries = job_config['properties']['max_retries']
        self.poll_interval = job_config['properties']['poll_interval']
        self.failure_threshold = job_config['properties']['failure_threshold']
        self.failure_threshold_activation = job_config['properties']['failure_threshold_activation']
        self.slack_channel = job_config['properties']['slack']['channel']
        self.slack_token = job_config['properties']['slack']['token']
        self.exp_backoff_factor = job_config['properties']['exp_backoff_factor']

        self.start_time = time.time()
        self.workdir = os.path.join(self.log_dir, str(uuid.uuid4()))
        self.work_packages = []
        self.n_wps = None
        self.slack_thread_id = None

        self._safely_mkdir(self.log_dir)
        self._safely_mkdir(self.workdir)


    def main(self):
        self.init_queue()
        self.notify_start()
        self.init_logger()

        while self.pending_work():
            self.schedule()
            self.wait()
            self.monitor()
            self.notify_status()

        self.persist_results()
        self.notify_done()
        self.cleanup()


    def init_logger(self):
        if self.slack_channel and self.slack_token:
            if self.slack_thread_id is None:
                logger.warning('No Slack thread id specified. Slack notifications will be send to channel directly.')

            SlackLoggingHandler.add_to_logger(logger, self.slack_channel, self.slack_token, self.slack_thread_id)


    def init_queue(self):
        logger.info('Initialized queue...')
        for param_file in self.param_files:
            for params in self._get_work_params(param_file):
                try:
                    resource_conf = config.get_resource_config(self.job_config, params)
                    wp = WorkPackage(params, **resource_conf)

                except Exception as e:
                    logger.error(f'Failed to initialize work package for {params}: {e}')
                    wp = WorkPackage.init_failed(params, str(e))

                self.work_packages.append(wp)

        self.n_wps = len(self.work_packages)
        self._persist_work_status()


    def schedule(self):
        logger.info(f'Scheduling {len(self.queued_work())}/{self.n_wps} new work packages...')

        for wps in self._groupby_resource_allocation(self.queued_work()):
            for wps_chunk in self._slurm_chunks(wps):
                self._submit_work(wps_chunk)


    def monitor(self):
        logger.info(f'Monitoring remaining {len(self.scheduled_work())}/{self.n_wps} work packages...')

        for wp in self.scheduled_work():

            try:
                s = wp.slurm_status = slurm.status(wp.job_id)

            except SlurmException as e:
                logger.error(f'Failed to determine Slurm job status: {e}')
                self._decommission(wp, str(e))
                continue

            if s is Status.COMPLETED:
                self._process_success(wp)

            elif s is Status.TIMEOUT:
                self._process_timeout(wp)

            elif s is Status.OUT_OF_MEMORY:
                self._process_oom(wp)

            elif s in slurm.RETRYABLE:
                self._process_retry(wp)

            elif s is Status.FAILED:
                self._process_failure(wp)

            elif s is Status.CANCELLED:
                self._process_cancellation(wp)

            elif s in slurm.ACTIVE:
                pass

            else:
                self._process_unknown_status(wp)

        if self._failure_threshold_reached():
            logger.critical(f'Failure threshold of {self.failure_threshold} reached. Cancelling all Slurm jobs and aborting the pipeline run...')
            self._panic()


    def wait(self):
        logger.info(f'Waiting {self.poll_interval}s until new poll...')
        time.sleep(self.poll_interval)


    def persist_results(self):
        logger.info('All pending work processed. Persisting results...')
        self._persist_work_status(WorkPackage.Status.SUCCEEDED)
        self._persist_work_status(WorkPackage.Status.FAILED)


    def notify_start(self):
        if self.slack_channel and self.slack_token:
            msg = '*PIPELINE JOB STARTED*\n'
            msg += f'> ⌛  Slurm {self.job_name} job is being scheduled...\n'
            msg += f'> 🌎  Scheduled param_file: {", ".join(self._param_files_names())}'
            msg += f' (for {self.left_over} left over).\n' if self.left_over else '.\n'
            self._notify(msg, thread=False)

            if len(self.failed_work()) > 0:
                msg = f'🚨  {len(self.failed_work())} of {self.n_wps} work packages could not be initialized and are marked as failed.'
                self._notify(msg)


    def notify_status(self):
        if not self._every_n_polls(25):
            return

        msg = f'*Status update after {self._strf_duration()}*\n'
        msg += f'> TOTAL: {self.n_wps}\n'
        msg += f'> PENDING: {len(self.pending_work())}\n'
        msg += f'> SUCCEEDED: {len(self.succeeded_work())}\n'
        msg += f'> FAILED: {len(self.failed_work())}\n'

        failure_causes = [wp.slurm_status.name for wp in self.failed_work() if wp.slurm_status]
        for k, v in Counter(failure_causes).most_common():
            msg += f'>   > slurm {k.lower()}: {v}\n'

        self._notify(msg)


    def notify_done(self):
        msg = '*PIPELINE JOB FINISHED*\n'
        msg += f'> 🏁  Slurm {self.job_name} job finished after {self._strf_duration()} hours.\n'
        msg += f'> 🌎  Processed param_file: {", ".join(self._param_files_names())}'
        msg += f' (for {self.left_over} left over).\n' if self.left_over else '.\n'
        msg += f'> 🎉  {len(self.succeeded_work())} of {self.n_wps} work packages succeeded.'

        self._notify(msg, thread=False)


    def cleanup(self):
        logger.info('Cleaning up temporary resources...')
        if not self.keep_work_dir:
            shutil.rmtree(self.workdir)


    def scheduled_work(self):
        return [wp for wp in self.pending_work() if wp.job_id is not None]


    def queued_work(self):
        return [wp for wp in self.pending_work() if wp.job_id is None]


    def pending_work(self):
        return [wp for wp in self.work_packages if wp.status == WorkPackage.Status.PENDING]


    def succeeded_work(self):
        return [wp for wp in self.work_packages if wp.status == WorkPackage.Status.SUCCEEDED]


    def failed_work(self):
        return [wp for wp in self.work_packages if wp.status == WorkPackage.Status.FAILED]


    def _process_success(self, wp):
        wp.status = WorkPackage.Status.SUCCEEDED
        logger.debug(f'Job {wp.name} ({wp.job_id}) succeeded. Removing job from queue.')
        self._persist_work_status()


    def _process_failure(self, wp):
        self._decommission(wp)
        logger.error(f'Unexpected error occurred for job {wp.name} ({wp.job_id}). Removing job from queue.')


    def _process_cancellation(self, wp):
        if self._oom_cancellation(wp):
            self._process_oom(wp)
        else:
            self._decommission(wp)
            logger.error(f'Job {wp.name} ({wp.job_id}) was canceled. Reason unknown. Removing job from queue.')


    def _process_timeout(self, wp):
        wp.time = wp.minutes() * self.exp_backoff_factor
        logger.error(f'Job {wp.name} ({wp.job_id}) ran into timeout. Rescheduling with {self.exp_backoff_factor}x higher timeout: {wp.time}.')
        self._requeue_work(wp)


    def _process_oom(self, wp):
        if wp.cpus >= slurm.MAX_CPUS:
            logger.error(f'Job {wp.name} ({wp.job_id}) ran out of memory, but has already been allocated the maximum number of cpus ({slurm.MAX_CPUS}). Rescheduling not possible. Removing job from queue.')
            self._decommission(wp)
        else:
            wp.cpus *= self.exp_backoff_factor
            logger.error(f'Job {wp.name} ({wp.job_id}) ran out of memory. Rescheduling with {self.exp_backoff_factor}x higher cpu count: {wp.cpus}.')
            self._requeue_work(wp)


    def _process_unknown_status(self, wp):
        self._decommission(wp)
        logger.error(f'Unknown Slurm status {wp.slurm_status} for job {wp.name} ({wp.job_id}). Removing job from queue.')


    def _process_retry(self, wp):
        self._requeue_work(wp)


    def _oom_cancellation(self, wp):
        with open(wp.stderr_log) as f:
            return 'Exceeded job memory limit' in f.read()


    def _decommission(self, wp, error_msg=None):
        if error_msg:
            wp.error_msg = error_msg
        wp.status = WorkPackage.Status.FAILED
        self._persist_work_status()


    def _panic(self):
        for wp in self.queued_work():
            wp.error_msg = 'Panic! All work packages in queue were canceled.'
            wp.status = WorkPackage.Status.FAILED

        for wp in self.scheduled_work():
            wp.error_msg = 'Panic! All running jobs were canceled.'
            wp.status = WorkPackage.Status.FAILED
            slurm.cancel(wp.job_id)

        self._persist_work_status()


    def _requeue_work(self, wp):
        if wp.n_tries >= self.max_retries:
            logger.error(f'Work package for {wp.params} failed to schedule after {self.max_retries} retries. Removing from queue.')
            self._decommission(wp)
            return

        if wp.job_id is not None:
            wp.old_job_ids.append(wp.job_id)
            wp.job_id = None


    def _submit_work(self, wps):
        workfile = self._persist_workfile(wps)
        array_conf = f'0-{len(wps)-1}'  # --array=0-0 is valid
        cpus = wps[0].cpus
        time = wps[0].time

        logger.debug(f'Scheduling Slurm job for {len(wps)} work packages with {cpus} cpus and a time limit of {time}...')

        try:
            job_ids = slurm.sbatch_array(
                workfile,
                array=array_conf,
                script=self.script,
                conda_env=self.conda_env,
                cpus=wps[0].cpus,
                time=wps[0].time,
                qos=wps[0].qos,
                partition=wps[0].partition,
                job_name=self.job_name,
                log_dir=self.log_dir,
                account=self.account,
                error='%x_%A_%a.stderr',
                output='%x_%A_%a.stdout'
                )

            for wp in wps:
                wp.n_tries += 1
                wp.job_id = job_ids.pop(0)
                wp.stdout_log = os.path.join(self.log_dir, f'{self.job_name}_{wp.job_id}.stdout')
                wp.stderr_log = os.path.join(self.log_dir, f'{self.job_name}_{wp.job_id}.stderr')

        except SlurmException as e:
            logger.critical(f'Failed to submit Slurm job array: {e}')
            for wp in wps:
                self._decommission(wp, str(e))


    def _persist_workfile(self, wps):
        workfile = os.path.join(self.workdir, f'{uuid.uuid4()}-workfile.txt')
        wp_params = [wp.params for wp in wps]

        with open(workfile, 'w', encoding='utf8') as f:
            json.dump(wp_params, f, indent=2, ensure_ascii=False)

        return workfile


    def _persist_work_status(self, status=None):
        wps = [wp.encode() for wp in self.work_packages if status is None or status == wp.status]

        filename = f'{status.name.lower()}-work.json' if status else 'work.json'
        filepath = os.path.join(self.log_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(wps, f, sort_keys=True, indent=4, ensure_ascii=False)


    def _get_work_params(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if 'yml' in file_path[-3:] or 'yaml' in file_path[-4:]:
                    params = yaml.safe_load(f)
                elif 'json' in file_path[-4:]:
                    params = json.load(f)
                elif 'csv' in file_path[-3:]:
                    params = list(csv.DictReader(f))
                else:
                    raise UsageError(f'Unsupported file type. Please specify param_file of type YAML, JSON, or CSV.')

        except FileNotFoundError:
            logger.critical(f'Could not find workfile {file_path}.')
            return []

        # TODO support left_over param_file for yaml param files as well
        # if self.left_over:
        #     unprocessed_paths = [path for path in paths if not os.path.isfile(f'{path}_{self.left_over}.csv')]
        #     logger.info(f'{len(unprocessed_paths)} of {len(paths)} *_{self.left_over}.csv paths left over from previous run.')
        #     return unprocessed_paths

        return params


    def _notify(self, msg, thread=True):
        if self.slack_channel and self.slack_token:
            try:
                thread_id = self.slack_thread_id if thread else None
                self.slack_thread_id = slack.send_message(msg, self.slack_channel, self.slack_token, thread_id)
            except Exception as e:
                logger.error(f'Failed to send Slack message: {e}')
        else:
            logger.info(f'No notification hook configured. Cannot send message {msg}.')
            logger.info('Consider adding a Slack channel and token to the config.')


    def _failure_threshold_reached(self):
        n_processed_wps = len(self.failed_work()) + len(self.succeeded_work())
        if n_processed_wps < self.failure_threshold_activation:
            return False

        failure_rate = len(self.failed_work()) / n_processed_wps
        return failure_rate >= self.failure_threshold


    def _every_n_polls(self, n):
        base = self.poll_interval
        rounded_duration = base * round(self._duration() / base)
        return rounded_duration % (base * n) == 0


    def _safely_mkdir(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)


    def _strf_duration(self):
        return str(datetime.timedelta(seconds=self._duration())).split('.')[0]


    def _param_files_names(self):
        return [os.path.basename(p) for p in self.param_files]


    def _duration(self):
        return time.time() - self.start_time


    def _slurm_chunks(self, wps):
        """Yield successive n-sized wps chunks according to Slurm's MaxArraySize."""
        n = slurm.MAX_ARRAY_SIZE
        for i in range(0, len(wps), n):
            yield wps[i:i + n]


    def _groupby_resource_allocation(self, wps):
        groups = defaultdict(list)
        for wp in wps:
            groups[(wp.cpus, wp.time)].append(wp)

        logger.debug(f'Work was grouped in {len(list(groups.values()))} groups with {list(groups.keys())} cpu & timeout configurations respectively.')
        yield from groups.values()
