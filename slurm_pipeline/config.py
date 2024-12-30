import os
import re
import glob
import json
import logging
import textwrap
from pathlib import Path

import yaml
import jsonschema

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_POLL_INTERVAL = 30
DEFAULT_EXP_BACKOFF_FACTOR = 4
DEFAULT_MAX_RETRIES = 3
DEFAULT_FAILURE_THRESHOLD = 0.25
DEFAULT_FAILURE_THRESHOLD_ACTIVATION = 50
DEFAULT_KEEP_WORK_DIR = False
DEFAULT_SLACK_CHANNEL = None
DEFAULT_SLACK_TOKEN = None
DEFAULT_ACCOUNT = None
DEFAULT_PARAM_FILES = []
DEFAULT_PARAM_GENERATOR_FILE = None
DEFAULT_N = None

SCHEMA_PROPERTIES = """
properties:
    type: object
    properties:
        conda_env:
            type: string
            description: Conda environment.
        account:
            type: [string, 'null']
            description: Slurm account.
        log_level:
            type: string
            description: Log level.
            enum:
            - DEBUG
            - INFO
            - WARN
            - ERROR
        keep_work_dir:
            type: boolean
            description: Keep the work directory after the job has finished.
        exp_backoff_factor:
            type: integer
            description: Exponential backoff factor for retries.
        max_retries:
            type: integer
            description: Maximum number of retries.
            minimum: 0
        poll_interval:
            type: integer
            description: Poll interval in seconds.
            minimum: 10
            maximum: 3600
        failure_threshold:
            type: number
            description: Relative threshold of processed work packages after which all Slurm jobs are canceled and the pipeline run is aborted.
            minimum: 0
            maximum: 1
        failure_threshold_activation:
            type: integer
            minimum: 1
            description: Number of initial work packages to be processed before the failure threshold is evaluated for the first time.
        slack:
            type: object
            required: [channel, token]
            properties:
                channel:
                    type: [string, 'null']
                    description: Slack channel name or ID.
                token:
                    type: [string, 'null']
                    description: Slack API token.
"""

SCHEMA = f"""
type: object
properties:

    jobs:
        type: array
        items:
            type: object
            required: [name, script, log_dir, resources]
            properties:
                name:
                    type: string
                    description: Descriptive name of the job.
                script:
                    type: string
                    description: Absolute path to the Python script to run.
                param_files:
                    type: array
                    description: Absolute paths to files that list the params.
                param_generator_file:
                    type: [string, 'null']
                    description: Absolute path to file that defines all param combinations.
                n:
                    type: [integer, 'null']
                    description: "Number of parameter combinations per param_file for which to schedule slurm tasks (default: all)."
                log_dir:
                    type: string
                    description: Absolute path to store the logs.
                resources:
                    type: object
                    required: [cpus]
                    properties:
                        cpus:
                            type: integer
                            description: Number of CPUs to request (4GB of memory per CPU).
                        mem:
                            type: integer
                            description: Size of required memory in MB.
                        time:
                            type: string
                            description: "Time limit per work package (format: days-hours:min:sec)."
                        partition:
                            type: string
                            description: Slurm partition.
                special_cases:
                    type: array
                    items:
                        type: object
                        properties:
                            name:
                                type: string
                                description: Name of the special case (documenting nature only).
                            resources:
                                type: object
                                properties:
                                    cpus:
                                        type: integer
                                        description: Number of CPUs to request (4GB of memory per CPU).
                                    mem:
                                        type: integer
                                        description: Size of required memory in MB.
                                    time:
                                        type: string
                                        description: "Time limit per work package (format: days-hours:min:sec)."
                                    partition:
                                        type: string
                                        description: Slurm partition.
                            files:
                                type: object
                                required: [path]
                                properties:
                                    path:
                                        type: string
                                        description: Path to file or directory to consider for special case assessment (may include variables from workfile with {{var}}).
                                    size_min:
                                        type: integer
                                        description: Minimum file / directory size.
                                    size_max:
                                        description: Maximum file / directory size.
                                        type: integer

                {textwrap.indent(SCHEMA_PROPERTIES, ' ' * 16)}

    {textwrap.indent(SCHEMA_PROPERTIES, ' ' * 4)}
"""

logger = logging.getLogger(__name__)


class UsageError(Exception):
    pass


def load(path):
    config = _load_yaml_file(path)
    _validate(config)
    _set_defaults(config)
    _merge_defaults(config)

    logger.info('Successfully interpolated config:\n' + json.dumps(config, indent=2))
    return config


def get_job_config(config, job_name):
    return next(job_conf for job_conf in config['jobs'] if job_conf['name'] == job_name)


def get_resource_config(job_config, params):
    default_resource_config = job_config['resources']

    for sc in job_config.get('special_cases', []):

        if file_config := sc.get('files'):
            path = _interpolate_variables(file_config['path'], params)
            size = _files_size(path)
            min = file_config.get('size_min', 0)
            max = file_config.get('size_max', float('inf'))

            if size >= min and size <= max:
                return {**default_resource_config, **sc['resources']}

    return default_resource_config


def _interpolate_variables(path, params):
    variables = re.findall(r'{{(.*?)}}', path)
    for v in variables:
        path = path.replace('{{%s}}' % v, params.get(v))

    return path


def _files_size(path):
    if  '*' in path:
        files = glob.glob(path, recursive=True)
    elif os.path.isdir(path):
        files = Path(path).rglob('*')
    else:
        files = [path]

    return sum(os.path.getsize(f) for f in files if os.path.isfile(f))


def _load_yaml_file(path):
    with open(path, 'r') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise UsageError(f'Error loading config {path}:\n{e}')


def _validate(config):
    try:
        jsonschema.validate(config, yaml.safe_load(SCHEMA))
    except jsonschema.exceptions.ValidationError as e:
        raise UsageError(f'Error validating config:\n{e.message} for {e.json_path}.')

    _validate_param_files(config)
    _validate_property_conda_env(config)


def _validate_param_files(config):
    for job_conf in config['jobs']:
        if bool(job_conf.get('param_files')) == bool(job_conf.get('param_generator_file')):  # XNOR
            raise UsageError(f"Either param_files or param_generator_file must be specified for job {job_conf['name']}.")


def _validate_property_conda_env(config):
    if not config.get('properties', {}).get('conda_env'):
        for job_conf in config['jobs']:
            if not job_conf.get('properties', {}).get('conda_env'):
                raise UsageError(f"The conda_env must be specified either in the global properties section or within each jobs' property section.")


def _set_defaults(config):
    config['properties'] = config.get('properties', {})
    config['properties']['account'] = config['properties'].get('account', DEFAULT_ACCOUNT)
    config['properties']['log_level'] = config['properties'].get('log_level', DEFAULT_LOG_LEVEL)
    config['properties']['max_retries'] = config['properties'].get('max_retries', DEFAULT_MAX_RETRIES)
    config['properties']['keep_work_dir'] = config['properties'].get('keep_work_dir', DEFAULT_KEEP_WORK_DIR)
    config['properties']['poll_interval'] = config['properties'].get('poll_interval', DEFAULT_POLL_INTERVAL)
    config['properties']['exp_backoff_factor'] = config['properties'].get('exp_backoff_factor', DEFAULT_EXP_BACKOFF_FACTOR)
    config['properties']['failure_threshold'] = config['properties'].get('failure_threshold', DEFAULT_FAILURE_THRESHOLD)
    config['properties']['failure_threshold_activation'] = config['properties'].get('failure_threshold_activation', DEFAULT_FAILURE_THRESHOLD_ACTIVATION)
    config['properties']['slack'] = config['properties'].get('slack', {})
    config['properties']['slack']['channel'] = config['properties']['slack'].get('channel', DEFAULT_SLACK_CHANNEL)
    config['properties']['slack']['token'] = config['properties']['slack'].get('token', DEFAULT_SLACK_TOKEN)

    for job in config['jobs']:
        job['n'] = job.get('n', DEFAULT_N)
        job['param_files'] = job.get('param_files', DEFAULT_PARAM_FILES)
        job['param_generator_file'] = job.get('param_generator_file', DEFAULT_PARAM_GENERATOR_FILE)


def _merge_defaults(config):
    for job_conf in config['jobs']:
        job_conf['properties'] = {**config['properties'], **job_conf.get('properties', {})}
