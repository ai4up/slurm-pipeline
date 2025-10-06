import os
import uuid
import csv
import json
import time
import shutil
import logging
import datetime
import itertools
import subprocess
from enum import Enum
from pathlib import Path
from collections import defaultdict, Counter

import yaml

from slurm_pipeline import config
from slurm_pipeline import slurm
from slurm_pipeline.config import UsageError
from slurm_pipeline.slurm import Status, SlurmException, SlurmConfig
from slurm_pipeline.slack_notifications import SlackLoggingHandler
import slurm_pipeline.slack_notifications as slack

logger = logging.getLogger(__name__)


def main(config):
    for job_config in config['jobs']:
        scheduler = Scheduler(job_config)
        scheduler.main()


class WorkPackage():

    Status = Enum('STATUS', 'PENDING FAILED SUCCEEDED')

    def __init__(self, params, cpus, mem=0, time=None, partition=None):
        self.params = params
        self.cpus = cpus
        self.mem = mem
        self.time = time
        self.partition = partition
        self.n_tries = 0
        self.name = 'TBD' #params['name']
        self.status = WorkPackage.Status.PENDING
        self.slurm_status = None
        self.error_msg = None
        self.stderr = None
        self.stdout = None
        self.mem_profile = None
        self.max_mem = None
        self.job_id = None
        self.old_job_ids = []


    @staticmethod
    def init_failed(params, error_msg):
        wp = WorkPackage(params, cpus=0)
        # TODO: introduce INIT_FAILED (& ABORTED) status in order to differentiate between runtime and init failures when assessing if failure threshold has been reached
        wp.status = WorkPackage.Status.FAILED
        wp.error_msg = error_msg
        return wp


    def encode(self):
        return {
            'params': self.params,
            'cpus': self.cpus,
            'mem': self.mem,
            'time': self.time,
            'partition': self.partition,
            'name': self.name,
            'status': self.status.name,
            'slurm_status': self.slurm_status.name if self.slurm_status else None,
            'n_tries': self.n_tries,
            'job_id': self.job_id,
            'stdout': self.stdout,
            'stderr': self.stderr,
            'mem_profile': self.mem_profile,
            'max_mem': self.max_mem,
            'error_msg': self.error_msg,
            'old_job_ids': self.old_job_ids
        }


    def to_json(self):
        return json.dumps(self.encode(), sort_keys=True, indent=4, ensure_ascii=False).encode('utf8')


class Scheduler():

    def __init__(self, job_config):
        self.job_config = job_config
        self.job_name = job_config['name']
        self.script = job_config['script']
        self.param_files = job_config['param_files']
        self.param_generator_file = job_config['param_generator_file']
        self.n = job_config['n']
        self.log_dir = os.path.join(job_config['log_dir'], f'{self.job_name}-{self._current_time()}')

        self.conda_env = job_config['properties']['conda_env']
        self.account = job_config['properties']['account']
        self.keep_work_dir = job_config['properties']['keep_work_dir']
        self.max_retries = job_config['properties']['max_retries']
        self.poll_interval = job_config['properties']['poll_interval']
        self.failure_threshold = job_config['properties']['failure_threshold']
        self.failure_threshold_activation = job_config['properties']['failure_threshold_activation']
        self.slack_channel = job_config['properties']['slack']['channel']
        self.slack_token = job_config['properties']['slack']['token']
        self.exp_backoff_factor = job_config['properties']['exp_backoff_factor']

        self.start_time = time.time()
        self.workdir = os.path.join(self.log_dir, 'workdir')
        self.task_log_dir = os.path.join(self.log_dir, 'task-logs')
        self.work_packages = []
        self.n_wps = None
        self.slack_thread_id = None
        self.slack_thread_id_details = None
        self.failure_notification = True

        self._safely_mkdir(self.workdir)
        self._safely_mkdir(self.task_log_dir)


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
        for params in self._get_work_params():
            try:
                resource_conf = config.get_resource_config(self.job_config, params)
                wp = WorkPackage(params, **resource_conf)

            except Exception as e:
                logger.error(f'Failed to initialize work package for {params}: {e}')
                wp = WorkPackage.init_failed(params, str(e))

            self.work_packages.append(wp)

        self.n_wps = len(self.work_packages)
        self.n_init_failed = len(self.failed_work())
        self._persist_work_status()

        if self.n_wps == 0:
            logger.error('No work packages specified. Pipeline will stop automatically...')
            return

        if self._init_failure_threshold_reached():
            logger.critical(f'Failure threshold of {self.failure_threshold} reached during work initialization. No Slurm jobs will be scheduled. Aborting the pipeline run...')
            self._panic()


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

            self._monitor_mem(wp)

        self._persist_work_status()

        if self._runtime_failure_threshold_reached():
            logger.critical(f'Failure threshold of {self.failure_threshold} reached. Cancelling all Slurm jobs and aborting the pipeline run...')
            self._panic()


    def wait(self):
        if self._duration() < 300:
            time.sleep(3)
        else:
            logger.info(f'Waiting {self.poll_interval}s until new poll...')
            time.sleep(self.poll_interval)


    def persist_results(self):
        logger.info('All pending work processed. Persisting results...')
        self._persist_work_status(WorkPackage.Status.SUCCEEDED)
        self._persist_work_status(WorkPackage.Status.FAILED)


    def notify_start(self):
        msg = self._status_msg()
        self.slack_thread_id, self.slack_channel = self._notify(msg)

        work_status = self._format_work_status()
        if len(work_status) < 2000:
            self.slack_thread_id_details, _ = self._notify(work_status)
        else:
            logger.info('Work status is too long to send as a Slack message.')


        if len(self.failed_work()) > 0:
            msg = f'🚨  {len(self.failed_work())} of {self.n_wps} work packages could not be initialized and are marked as failed.'
            self._notify(msg)


    def notify_status(self):
        msg = self._status_msg()
        self._notify(msg, update=True)

        if self.slack_thread_id_details:
            work_status = self._format_work_status()
            self._notify(work_status, update=True, thread_id=self.slack_thread_id_details)

        if self._every_n_polls(100) and len(self.failed_work()) > 0:
            msg = self._failure_summary()
            self._notify(msg)


    def notify_failure(self, wp):
        stderr = self._read_log(wp.stderr)
        stdout = self._read_log(wp.stdout)

        msg = f'*Work package {wp.job_id} failed*\n'
        msg += f'Slurm status: `{wp.slurm_status.name}`\n'
        msg += f'Retries: `{wp.n_tries - 1}`\n'

        if wp.error_msg:
            msg += f'Error msg: `{wp.error_msg}`\n'

        if stderr:
            stderr_tail = '\n'.join(stderr.splitlines()[-50:])
            msg += f'Stderr log: ```{stderr_tail}```\n'

        if stdout:
            stdout_tail = '\n'.join(stdout.splitlines()[-50:])
            msg += f'Stdout log: ```{stdout_tail}```\n'

        if self.failure_notification:
            msg += '<!channel>'
            self.failure_notification = False

        self._notify(msg)


    def notify_done(self):
        self.notify_status()
        self._notify(self._failure_summary())
        self._notify('<!channel> *PIPELINE JOB FINISHED*\n')


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


    def _monitor_mem(self, wp):
        if not wp.mem_profile or not os.path.isfile(wp.mem_profile):
            logger.debug(f'No memory profile file found for work package. Not scheduled yet or mprof is not installed')
            return

        cmd = f'mprof peak {wp.mem_profile}'
        p = subprocess.run(cmd, capture_output=True, shell=True)
        if p.returncode == 0:
            wp.max_mem = p.stdout.decode('UTF-8').split()[1]
        else:
            logger.info(f'Determining memory peak not possible. mprof cmd failed with: {p.stderr.decode("UTF-8")}')


    def _process_success(self, wp):
        wp.status = WorkPackage.Status.SUCCEEDED
        logger.debug(f'Job {wp.name} ({wp.job_id}) succeeded. Removing job from queue.')


    def _process_failure(self, wp):
        self._decommission(wp)
        self.notify_failure(wp)
        logger.error(f'Unexpected error occurred for job {wp.name} ({wp.job_id}). Removing job from queue.')


    def _process_cancellation(self, wp):
        if self._oom_cancellation(wp):
            self._process_oom(wp)
        else:
            self._decommission(wp)
            logger.error(f'Job {wp.name} ({wp.job_id}) was canceled. Reason unknown. Removing job from queue.')


    def _process_timeout(self, wp):
        wp.time = slurm.minutes(wp.time) * self.exp_backoff_factor
        logger.error(f'Job {wp.name} ({wp.job_id}) ran into timeout. Trying to reschedule with {self.exp_backoff_factor}x higher timeout: {wp.time}.')
        self._requeue_work(wp)


    def _process_oom(self, wp):
        max_mem = slurm.GPU_MAX_MEM if wp.partition == 'gpu' else slurm.MAX_MEM
        mem_per_cpu = slurm.GPU_MEM_PER_CPU if wp.partition == 'gpu' else slurm.MEM_PER_CPU
        wp.mem = wp.mem or wp.cpus * mem_per_cpu

        if wp.mem >= max_mem:
            msg = f'Job {wp.name} ({wp.job_id}) ran out of memory, but has already been allocated the maximum available memory ({max_mem}). Rescheduling not possible. Removing job from queue.'
            logger.error(msg)
            wp.error_msg = msg
            self._decommission(wp)
        else:
            wp.mem *= self.exp_backoff_factor
            logger.error(f'Job {wp.name} ({wp.job_id}) ran out of memory. Trying to reschedule with {self.exp_backoff_factor}x higher memory: {wp.mem}.')
            self._requeue_work(wp)


    def _process_unknown_status(self, wp):
        self._decommission(wp)
        logger.error(f'Unknown Slurm status {wp.slurm_status} for job {wp.name} ({wp.job_id}). Removing job from queue.')


    def _process_retry(self, wp):
        self._requeue_work(wp)


    def _oom_cancellation(self, wp):
        return 'Exceeded job memory limit' in self._read_log(wp.stderr)


    def _decommission(self, wp, error_msg=None):
        if error_msg:
            wp.error_msg = error_msg
        wp.status = WorkPackage.Status.FAILED


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
        if wp.n_tries > self.max_retries:
            logger.error(f'Max retries reached ({wp.n_tries-1}/{self.max_retries}). Removing work package for {wp.params} from queue.')
            self._decommission(wp)
            return

        if wp.job_id is not None:
            wp.old_job_ids.append(wp.job_id)
            wp.job_id = None


    def _submit_work(self, wps):
        workfile = self._persist_workfile(wps)
        array_conf = f'0-{len(wps)-1}' if len(wps) > 1 else None
        log_file_id = '%A_%a' if array_conf else '%A'
        cpus = wps[0].cpus
        time = wps[0].time
        mem = wps[0].mem
        partition = wps[0].partition

        logger.debug(f'Scheduling Slurm job for {len(wps)} work packages with {cpus} cpus, {mem}MB memory, and a time limit of {time} on partition {partition}...')

        try:
            slurm_conf = SlurmConfig(
                time=time,
                cpus=cpus,
                mem=mem,
                array=array_conf,
                partition=partition,
                account=self.account,
                job_name=self.job_name,
                log_dir=self.task_log_dir,
                error=f'{log_file_id}.stderr',
                output=f'{log_file_id}.stdout',
            )
            job_id, task_ids = slurm.sbatch_workfile(
                workfile,
                script=self.script,
                conda_env=self.conda_env,
                slurm_conf=slurm_conf,
                )
            logger.info(f'Successfully scheduled Slurm job {job_id} with array tasks {task_ids}')

            for i, wp in enumerate(wps):
                if len(wps) == 1:
                    wp.job_id = job_id
                    file_id = job_id
                elif task_ids:
                    # parallelized via slurm array
                    wp.job_id = task_ids.pop(0)
                    file_id = wp.job_id
                else:
                    # parallelized using bash's job control
                    wp.job_id = job_id
                    file_id = f'{job_id}_{i}'

                wp.stdout = os.path.join(self.task_log_dir, f'{file_id}.stdout')
                wp.stderr = os.path.join(self.task_log_dir, f'{file_id}.stderr')
                wp.mem_profile = os.path.join(self.task_log_dir, f'mprofile_{file_id}.dat')
                wp.n_tries += 1

        except SlurmException as e:
            logger.critical(f'Failed to submit Slurm job array: {e}')
            for wp in wps:
                self._decommission(wp, str(e))


    def _persist_workfile(self, wps):
        workfile = os.path.join(self.workdir, f'{uuid.uuid4()}-workfile.json')
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


    def _get_work_params(self):
        return self._read_param_files() if self.param_files else self._generate_params()


    def _read_param_files(self):
        all_params = []
        for path in self.param_files:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    if 'yml' in path[-3:] or 'yaml' in path[-4:]:
                        params = yaml.safe_load(f)
                    elif 'json' in path[-4:]:
                        params = json.load(f)
                    elif 'csv' in path[-3:]:
                        params = list(csv.DictReader(f))
                    else:
                        raise UsageError(f'Unsupported file type. Please specify param_file of type YAML, JSON, or CSV.')

            except FileNotFoundError:
                logger.critical(f'Could not find param file {path}.')
                return []

            if self.n:
                all_params += params[:self.n]
            else:
                all_params += params

        return all_params


    def _generate_params(self):
        path = self.param_generator_file
        with open(path, 'r', encoding='utf-8') as f:
            if 'yml' in path[-3:] or 'yaml' in path[-4:]:
                config = yaml.safe_load(f)
            elif 'json' in path[-4:]:
                config = json.load(f)
            else:
                raise UsageError(f'Unsupported file type. Please specify param_generator_file of type YAML or JSON.')

        for v in config.values():
            if type(v) is not list:
                raise UsageError(f'Malformated param_generator_file. Every attribute must be a list.')

        value_combinations = list(itertools.product(*config.values()))
        param_combinations = [dict(zip(config.keys(), p)) for p in value_combinations]

        return param_combinations


    def _status_msg(self):
        msg = f'*PIPELINE JOB {self.job_name.upper()}*\n'
        msg += f'> ⚙️  Scheduled param_files: {", ".join(self._param_files_names())}\n'

        if self.pending_work():
            msg += f'> ⌛  Running for {self._strf_duration()} hours...\n'
        else:
            msg += f'> 🏁  Finished after {self._strf_duration()} hours.\n'

        msg += f'> 🎉  {len(self.succeeded_work())} of {self.n_wps} work packages succeeded.\n'
        msg += f'> ❌  {len(self.failed_work())} of {self.n_wps} work packages failed.\n'
        return msg


    def _failure_summary(self):
        failure_causes = [wp.slurm_status.name for wp in self.failed_work() if wp.slurm_status]

        msg = 'Summary of most frequent Slurm failure status:\n'
        for k, v in Counter(failure_causes).most_common():
            msg += f'> {k.lower()}: {v}\n'

        return msg


    def _format_work_status(self):
        wps = [wp.encode() for wp in self.work_packages]
        msg = yaml.dump(wps, default_flow_style=False)
        return f'```{msg}```'


    def _notify(self, msg, thread_id=None, update=False):
        if self.slack_channel and self.slack_token:
            try:
                if thread_id is None:
                    thread_id = self.slack_thread_id

                if update:
                    return slack.update_message(msg, self.slack_channel, self.slack_token, thread_id)
                else:
                    return slack.send_message(msg, self.slack_channel, self.slack_token, thread_id)


            except Exception as e:
                logger.error(f'Failed to send Slack message: {e}')
        else:
            logger.debug(f'No notification hook configured. Cannot send message {msg}.')
            logger.debug('Consider adding a Slack channel and token to the config.')

        return None, None


    def _init_failure_threshold_reached(self):
        failure_rate = self.n_init_failed / self.n_wps
        return failure_rate >= self.failure_threshold


    def _runtime_failure_threshold_reached(self):
        n_failed_wps = len(self.failed_work()) - self.n_init_failed
        n_processed_wps = len(self.succeeded_work()) + n_failed_wps

        if n_processed_wps < self.failure_threshold_activation:
            return False

        failure_rate = n_failed_wps / n_processed_wps
        return failure_rate >= self.failure_threshold


    def _every_n_polls(self, n):
        base = self.poll_interval
        rounded_duration = base * round(self._duration() / base)
        return rounded_duration % (base * n) == 0


    def _safely_mkdir(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)


    def _strf_duration(self):
        return str(datetime.timedelta(seconds=self._duration())).rsplit(':', 1)[0]


    def _current_time(self):
        return datetime.datetime.now().strftime('%Y-%m-%d--%H-%M-%S')


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
            groups[(wp.cpus, wp.mem, wp.time, wp.partition)].append(wp)

        logger.debug(f'Work was grouped in {len(list(groups.values()))} groups with {list(groups.keys())} cpu, memory, timeout & partition configurations respectively.')
        yield from groups.values()


    def _read_log(self, path):
        try:
            return Path(path).read_text()
        except (FileNotFoundError, TypeError):
            return ''
