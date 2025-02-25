import os
import datetime
import subprocess
import logging
from enum import Enum

MAX_ARRAY_SIZE = 1001
MAX_CPUS = 128
MAX_MEM = 690000
MEM_PER_CPU = 5468
GPU_MAX_MEM = 690000
GPU_MEM_PER_CPU = 11328

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'slurm-templates')

logger = logging.getLogger(__name__)

class SlurmException(Exception):
    pass


class Status(Enum):
    BOOT_FAIL = 'BOOT_FAIL'
    CANCELLED = 'CANCELLED'
    COMPLETED = 'COMPLETED'
    CONFIGURING = 'CONFIGURING'
    COMPLETING = 'COMPLETING'
    DEADLINE = 'DEADLINE'
    FAILED = 'FAILED'
    NODE_FAIL = 'NODE_FAIL'
    OUT_OF_MEMORY = 'OUT_OF_MEMORY'
    PENDING = 'PENDING'
    PREEMPTED = 'PREEMPTED'
    RUNNING = 'RUNNING'
    RESV_DEL_HOLD = 'RESV_DEL_HOLD'
    REQUEUE_FED = 'REQUEUE_FED'
    REQUEUE_HOLD = 'REQUEUE_HOLD'
    REQUEUED = 'REQUEUED'
    RESIZING = 'RESIZING'
    REVOKED = 'REVOKED'
    SIGNALING = 'SIGNALING'
    SPECIAL_EXIT = 'SPECIAL_EXIT'
    STAGE_OUT = 'STAGE_OUT'
    STOPPED = 'STOPPED'
    SUSPENDED = 'SUSPENDED'
    TIMEOUT = 'TIMEOUT'
    UNKNOWN = 'UNKNOWN'

RETRYABLE = [Status.BOOT_FAIL, Status.NODE_FAIL, Status.REQUEUED, Status.REQUEUE_FED, Status.STOPPED, Status.SUSPENDED]
ACTIVE = [Status.PENDING, Status.RUNNING, Status.CONFIGURING, Status.COMPLETING, Status.RESIZING]

class SlurmConfig():

    def __init__(self,
                cpus=1,
                nodes=1,
                ntasks=1,
                error='%x_%j.stderr',
                output='%x_%j.stdout',
                mem=0,
                time=None,
                partition=None,
                gres=None,
                qos=None,
                account=None,
                array=None,
                job_name=None,
                log_dir=None,
                env_vars=None,
                ):

        self.cpus = cpus
        self.nodes = nodes
        self.ntasks = ntasks
        self.error = error
        self.output = output
        self.mem = mem
        self.time = time
        self.account = account
        self.array = array
        self.job_name = job_name
        self.log_dir = log_dir
        self.env_vars = env_vars

        self.partition = partition or self._determine_partition()
        self.gres = gres or self._determine_gres()
        self.qos = qos or self._determine_qos()


    def _determine_partition(self):
        return 'standard'


    def _determine_gres(self):
        if self.partition == 'gpu':
            return 'gpu'


    def _determine_qos(self):
        qos = ''
        if self.partition == 'io':
            return 'io'

        if self.partition == 'gpu':
            qos = 'gpu'

        if not self.time or minutes(self.time) > 24 * 60 * 7:
            qos += 'long'
        elif minutes(self.time) > 24 * 60:
            qos += 'medium'
        else:
            qos += 'short'

        return qos


    def array_size(self):
        return int(self.array.split('-')[1]) + 1 if self.array else 0


    def validate_and_adjust(self):
        if self.cpus > MAX_CPUS:
            logger.warning(f'Requesting {self.cpus} CPUs, but max allowed is {MAX_CPUS}. Reducing CPUs accordingly.')
            self.cpus = MAX_CPUS

        if self.partition == 'gpu' and self.mem > GPU_MAX_MEM:
            logger.warning(f'Requesting {self.mem}MB memory, but max allowed is {GPU_MAX_MEM}. Reducing memory accordingly.')
            self.mem = GPU_MAX_MEM

        elif self.mem > MAX_MEM:
            logger.warning(f'Requesting {self.mem}MB memory, but max allowed is {MAX_MEM}. Reducing memory accordingly.')
            self.mem = MAX_MEM

        if self.array and self.partition == 'io':
            self.array = None # schedule tasks subsequently on io partition as it does not support sbatch arrays


    def to_s(self):
        options = ''
        options += f' --nodes={self.nodes}'
        options += f' --error="{self.error}"'
        options += f' --output="{self.output}"'
        options += f' --ntasks={self.ntasks}'
        options += f' --cpus-per-task={self.cpus}'
        options += f' --qos={self.qos}'
        options += f' --partition={self.partition}'

        if self.time:
            options += f' --time="{self.time}"'
        if self.gres:
            options += f' --gres={self.gres}'
        if self.mem:
            options += f' --mem={self.mem}'
        if self.account:
            options += f' --account={self.account}'
        if self.array:
            options += f' --array={self.array}'
        if self.job_name:
            options += f' --job-name="{self.job_name}"'
        if self.log_dir:
            options += f' --chdir="{self.log_dir}"'
        if self.env_vars:
            options += f' --export=ALL,{self.env_vars}'

        return options


def sbatch(script, conda_env, slurm_conf, args='', workfile='', sbatch_script=None):
    if (workfile or slurm_conf.array) and sbatch_script is None:
        raise SlurmException(f'Default sbatch script does not support workfile and array configuration. Please pass a custom sbatch script.')

    slurm_conf.validate_and_adjust()
    options = slurm_conf.to_s()
    sbatch_script = sbatch_script or os.path.join(TEMPLATE_PATH, 'sbatch.sh')
    cmd = f'sbatch --parsable {options} "{sbatch_script}" "{conda_env}" "{script}" {args or workfile}'

    logger.debug(f'Submitting Slurm job with cmd: {cmd}')
    p = subprocess.run(cmd, capture_output=True, shell=True)

    stderr = p.stderr.decode('UTF-8')
    logger.info(f'stderr: {stderr}')

    if p.returncode > 0:
        raise SlurmException(f'Error running Slurm cmd {cmd}:\n{stderr}')

    job_id = p.stdout.decode('UTF-8').strip()
    return job_id


def sbatch_workfile(workfile, **kwargs):
    sbatch_script = os.path.join(TEMPLATE_PATH, 'sbatch-workfile.sh')
    job_id = sbatch(workfile=workfile, sbatch_script=sbatch_script, **kwargs)

    slurm_conf = kwargs.get('slurm_conf')
    task_ids = [f'{job_id}_{array_id}' for array_id in range(slurm_conf.array_size())]

    return job_id, task_ids


def status(job_id):
    logger.debug(f'Getting Slurm status for job {job_id}...')

    cmd = f'sacct --job={job_id} --format=state --parsable2 --noheader'
    p = subprocess.run(cmd, capture_output=True, shell=True)

    if p.returncode > 0:
        raise SlurmException(f'Error running Slurm cmd {cmd}:\n{p.stderr.decode("UTF-8")}')

    try:
        s = p.stdout.decode('UTF-8').splitlines()[0].split(maxsplit=1)[0]
    except IndexError:
        logger.warning(f'Could not determine status for job {job_id}. Maybe the job has not been submitted yet?')
        return Status.PENDING

    try:
        return Status(s)
    except ValueError:
        return Status.UNKNOWN


def cancel(job_id):
    logger.debug(f'Cancelling Slurm job {job_id}...')

    cmd = f'scancel {job_id}'
    p = subprocess.run(cmd, capture_output=True, shell=True)

    if p.returncode > 0:
        raise SlurmException(f'Error running Slurm cmd {cmd}:\n{p.stderr.decode("UTF-8")}')


def squeue(job=None, account=None):
    logger.debug(f'Squeueing Slurm jobs...')

    cmd = 'squeue --states=all'
    cmd += f' --name={job}' if job else ''
    cmd += f' --account={account}' if account else ''

    p = subprocess.run(cmd, capture_output=True, shell=True)

    if p.returncode > 0:
        raise SlurmException(f'Error running Slurm cmd {cmd}:\n{p.stderr.decode("UTF-8")}')

    return p.stdout.decode('UTF-8')


def minutes(time_str):
    timedelta = parse_time(time_str)
    return round(timedelta.total_seconds() / 60)


def parse_time(time_str):
    """
    Acceptable time formats include 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
    https://slurm.schedmd.com/sbatch.html#OPT_time
    """
    if time_str is None:
        return datetime.timedelta()

    d, h, m, s = 0, 0, 0, 0
    time_str = str(time_str)

    if '-' in time_str:
        d, time_str = time_str.split('-')

    t = time_str.split(':')

    if len(t) == 1 and d:
        h = t[0]
    elif len(t) == 2 and d:
        h = t[0]
        m = t[1]
    elif len(t) == 1:
        m = t[0]
    elif len(t) == 2:
        m = t[0]
        s = t[1]
    elif len(t) == 3:
        h = t[0]
        m = t[1]
        s = t[2]

    return datetime.timedelta(days=int(d), hours=int(h), minutes=int(m), seconds=int(s))
