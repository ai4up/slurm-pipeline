import os
import uuid
import json
import time
import shutil
import logging
import datetime
from enum import Enum
from pathlib import Path
from collections import defaultdict

import config
import slurm
from slurm import Status, SlurmException
import slack_notifications as slack

logger = logging.getLogger(__name__)


class WorkPackage():

    Status = Enum('STATUS', 'PENDING FAILED SUCCEEDED')

    def __init__(self, path, cpus, time):
        self.path = path
        self.cpus = cpus
        self.time = time
        self.n_tries = 0
        self.name = Path(path).stem
        self.status = WorkPackage.Status.PENDING
        self.slurm_status = None
        self.error_msg = None
        self.stderr_log = None
        self.stdout_log = None
        self.job_id = None
        self.old_job_ids = []


    @staticmethod
    def init_failed(path, error_msg):
        wp = WorkPackage(path, cpus=None, time=None)
        wp.status = WorkPackage.Status.FAILED
        wp.error_msg = error_msg
        return wp


    def encode(self):
        return {
            'path': self.path,
            'cpus': self.cpus,
            'time': self.time,
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


    def partition(self):
        return 'standard' if self.cpus <= 16 else 'broadwell'


    def qos(self):
        if self.minutes() <= 24 * 60:
            return 'short'
        elif self.minutes() <= 24 * 60 * 7:
            return 'medium'
        else:
            return 'long'


    def to_json(self):
        return json.dumps(self.encode(), sort_keys=True, indent=4, ensure_ascii=False).encode('utf8')


class Scheduler():

    def __init__(self, job_config):
        self.job_config = job_config
        self.job_name = job_config['name']
        self.script = job_config['script']
        self.log_dir = job_config['log_dir']
        self.data_dir = job_config['data_dir']
        self.countries = job_config['countries']

        self.conda_env = job_config['properties']['conda_env']
        self.account = job_config['properties']['account']
        self.left_over = job_config['properties']['left_over']
        self.keep_work_dir = job_config['properties']['keep_work_dir']
        self.max_retries = job_config['properties']['max_retries']
        self.poll_interval = job_config['properties']['poll_interval']
        self.slack_channel = job_config['properties']['slack']['channel']
        self.slack_token = job_config['properties']['slack']['token']
        self.exp_backoff_factor = job_config['properties']['exp_backoff_factor']

        self.start_time = time.time()
        self.workdir = os.path.join(self.log_dir, str(uuid.uuid4()))
        self.work_packages = []
        self.n_wps = None

        self._safely_mkdir(self.log_dir)
        self._safely_mkdir(self.workdir)


    def main(self):
        self.init_queue()

        while self.pending_work():
            self.schedule()
            self.wait()
            self.monitor()

        self.persist_results()
        self.notify()
        self.cleanup()


    def init_queue(self):
        logger.info('Initialized queue...')
        for country in self.countries:
            for path in self._get_work_paths(country, self.data_dir, self.left_over):
                try:
                    resource_conf = config.get_resource_config(path, self.job_config)
                    wp = WorkPackage(path, **resource_conf)

                except Exception as e:
                    logger.error(f'Failed to initialize work package for {path}: {e}')
                    wp = WorkPackage.init_failed(path, str(e))

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


    def wait(self):
        logger.info(f'Waiting {self.poll_interval}s until new poll...')
        time.sleep(self.poll_interval)


    def persist_results(self):
        logger.info('All pending work processed. Persisting results...')
        self._persist_work_status(WorkPackage.Status.SUCCEEDED)
        self._persist_work_status(WorkPackage.Status.FAILED)


    def notify(self):
        if self.slack_channel and self.slack_token:
            duration =  str(datetime.timedelta(seconds=time.time() - self.start_time)).split('.')[0]
            msg = f'⌛  Slurm {self.job_name} job finished after {duration} hours.\n'
            msg += f'🌎  Processed countries: {", ".join(self.countries)}'
            msg += f' (for {self.left_over} left over).\n' if self.left_over else '.\n'
            msg += f'🎉  {len(self.succeeded_work())} of {self.n_wps} work packages succeeded.'
            slack.send_message(msg, self.slack_channel, self.slack_token)
        else:
            logger.info('No notification hook configured. Consider adding a Slack channel and token to the config.yml.')


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


    def _requeue_work(self, wp):
        if wp.n_tries >= self.max_retries:
            logger.error(f'Work package for {wp.path} failed to schedule after {self.max_retries} retries. Removing from queue.')
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
                qos=wps[0].qos(),
                partition=wps[0].partition(),
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
            logger.error(f'Failed to submit Slurm job array: {e}')
            for wp in wps:
                self._decommission(wp, str(e))


    def _persist_workfile(self, wps):
        workfile = os.path.join(self.workdir, f'{uuid.uuid4()}-workfile.txt')
        wp_paths = [wp.path for wp in wps]

        with open(workfile, 'w') as f:
            for item in wp_paths:
                f.write(f'{item}\n')

        return workfile


    def _persist_work_status(self, status=None):
        wps = [wp.encode() for wp in self.work_packages if status is None or status == wp.status]

        filename = f'{status.name.lower()}-work.json' if status else 'work.json'
        filepath = os.path.join(self.log_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(wps, f, sort_keys=True, indent=4, ensure_ascii=False)


    def _get_work_paths(self, country_name, data_dir, left_over=''):

        file_prefix = f'failed_{left_over}_' if left_over else ''
        file_path = os.path.join(data_dir, country_name, 'paths_' + file_prefix + country_name + '.txt')

        with open(file_path) as f:
            paths = [line.rstrip() for line in f]

        return paths


    def _safely_mkdir(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)


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

