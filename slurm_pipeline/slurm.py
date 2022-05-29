import os
import datetime
import subprocess
import logging
from enum import Enum
from pathlib import Path

MAX_ARRAY_SIZE = 3000

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

def sbatch(script,
            log_dir,
            conda_env='/home/nikolami/.conda/envs/ox112',
            qos='short',
            partition='standard',
            time='01:00:00',
            error='%x_%A_%a.stderr',
            output='%x_%A_%a.stdout',
            cpus=1,
            nodes=1,
            ntasks=1,
            workfile='',
            account='eubucco',
            array=None,
            job_name=None,
            mem=None,
            sbatch_script=None):
            # TODO: add support for other sbatch options
            # *args,
            # **kwargs):

    job_name = job_name or Path(script).stem
    sbatch_script = sbatch_script or os.path.join(os.path.dirname(os.path.abspath(__file__)), 'slurm-templates', 'sbatch.sh')

    options = ''
    options += f' --qos={qos}'
    options += f' --time="{time}"'
    options += f' --nodes={nodes}'
    options += f' --account={account}'
    options += f' --error="{error}"'
    options += f' --output="{output}"'
    options += f' --chdir="{log_dir}"'
    options += f' --ntasks={ntasks}'
    options += f' --cpus-per-task={cpus}'
    options += f' --partition={partition}'
    options += f' --job-name="{job_name}"'
    if mem:
        options += f' --mem={mem}'
    if array:
        options += f' --array={array}'

    cmd = f'sbatch --parsable {options} "{sbatch_script}" "{script}" "{conda_env}" {workfile}'
    logger.debug(f'Submitting Slurm job with cmd: {cmd}')

    p = subprocess.run(cmd, capture_output=True, shell=True)

    if p.returncode > 0:
        raise SlurmException(f'Error running sbatch cmd {cmd}:\n{p.stderr.decode("UTF-8")}')

    job_id = p.stdout.decode('UTF-8').strip()
    return job_id


def sbatch_array(workfile, array=None, **kwargs):
    array = array or _array_conf(workfile)
    sbatch_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'slurm-templates', 'sbatch-array.sh')

    job_id = sbatch(workfile=workfile, array=array, sbatch_script=sbatch_script, **kwargs)

    return [f'{job_id}_{array_id}' for array_id in range(int(array.split('-')[1]) + 1)]


def status(job_id):
    logger.debug(f'Getting Slurm status for job {job_id}...')

    cmd = f"sacct --job={job_id} --format=state --parsable2 --noheader"
    p = subprocess.run(cmd, capture_output=True, shell=True)

    if p.returncode > 0:
        raise SlurmException(f'Error running sacct cmd {cmd}:\n{p.stderr.decode("UTF-8")}')

    try:
        s = p.stdout.decode('UTF-8').splitlines()[0].strip()
    except IndexError:
        logger.warning(f'Could not determine status for job {job_id}. Maybe the job has not been submitted yet?')
        return Status.PENDING

    try:
        return Status(s)
    except ValueError:
        return Status.UNKNOWN


def parse_time(time_str):
    """
    Acceptable time formats include 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
    https://slurm.schedmd.com/sbatch.html#OPT_time
    """
    d, h, m, s = 0, 0, 0, 0

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


def _array_conf(workfile):
    n_lines = sum(1 for line in open(workfile))
    return f'0-{n_lines-1}'
