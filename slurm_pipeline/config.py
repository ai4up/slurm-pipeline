import os
import json
import logging
import textwrap

import yaml
import jsonschema

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_POLL_INTERVAL = 30
DEFAULT_EXP_BACKOFF_FACTOR = 4
DEFAULT_MAX_RETRIES = 3
DEFAULT_FAILURE_THRESHOLD = 0.25
DEFAULT_FAILURE_THRESHOLD_ACTIVATION = 50
DEFAULT_KEEP_WORK_DIR = False
DEFAULT_LEFT_OVER = None
DEFAULT_SLACK_CHANNEL = None
DEFAULT_SLACK_TOKEN = None
DEFAULT_ACCOUNT = None
DEFAULT_DEPENDENCIES = []

SCHEMA_PROPERTIES = """
properties:
    type: object
    properties:
        conda_env:
            type: string
            description: Conda environment.
        account:
            type: string
            description: Slurm account.
        log_level:
            type: string
            description: Log level.
            enum:
            - DEBUG
            - INFO
            - WARN
            - ERROR
        left_over:
            type: string
            description: Run job only for work packages, which have not been processed yet. Specify type / suffix of job output files (e.g. bld_fts).
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
                    type: string
                    description: Slack channel name or ID.
                token:
                    type: string
                    description: Slack API token.
"""

SCHEMA = f"""
type: object
properties:

    jobs:
        type: array
        items:
            type: object
            required: [name, script, workfiles, log_dir, resources]
            properties:
                name:
                    type: string
                    description: Descriptive name of the job.
                script:
                    type: string
                    description: Absolute path to the Python script to run.
                workfiles:
                    type: array
                    description: Absolute paths to workfiles.
                dependencies:
                    type: array
                    description: List of job names which have to finish before the job can be scheduled.
                log_dir:
                    type: string
                    description: Absolute path to store the logs.
                resources:
                    type: object
                    required: [cpus, time]
                    properties:
                        cpus:
                            type: integer
                            description: Number of CPUs to request (8GB of memory per CPU).
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
                                        description: Number of CPUs to request (8GB of memory per CPU).
                                    time:
                                        type: string
                                        description: "Time limit per work package (format: days-hours:min:sec)."
                                    partition:
                                        type: string
                                        description: Slurm partition.
                            file:
                                type: object
                                required: [type]
                                properties:
                                    type:
                                        type: string
                                        description: Type / suffix of file to consider for special case assessment.
                                    file_size_min:
                                        type: integer
                                        description: Minimum file size.
                                    file_size_max:
                                        description: Maximum file size.
                                        type: integer

                {textwrap.indent(SCHEMA_PROPERTIES, ' ' * 16)}

    {textwrap.indent(SCHEMA_PROPERTIES, ' ' * 4)}
"""

logger = logging.getLogger(__name__)


class UsageError(Exception):
    pass


def load(path):
    config = _load_config_yaml(path)
    _validate(config)
    _set_defaults(config)
    _merge_defaults(config)

    logger.info('Successfully interpolated config:\n' + json.dumps(config, indent=2))
    return config


def get_job_config(config, job_name):
    return next(job_conf for job_conf in config['jobs'] if job_conf['name'] == job_name)


def get_resource_config(base_path, job_config):
    default_resource_config = job_config['resources']

    for sc in job_config.get('special_cases', []):

        if file_config := sc.get('file'):
            path = f"{base_path}_{file_config['type']}"
            size = os.path.getsize(path)
            min = file_config.get('file_size_min', 0)
            max = file_config.get('file_size_max', float('inf'))

            if size >= min and size <= max:
                return {**default_resource_config, **sc['resources']}

    return default_resource_config


def _load_config_yaml(path):
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

    _validate_property_conda_env(config)


def _validate_property_conda_env(config):
    if not config.get('properties', {}).get('conda_env'):
        for job_conf in config['jobs']:
            if not job_conf.get('properties', {}).get('conda_env'):
                raise UsageError(f"The conda_env must be specified either in the global properties section or within each jobs' property section.")


def _set_defaults(config):
    config['properties'] = config.get('properties', {})
    config['properties']['account'] = config['properties'].get('account', DEFAULT_ACCOUNT)
    config['properties']['log_level'] = config['properties'].get('log_level', DEFAULT_LOG_LEVEL)
    config['properties']['left_over'] = config['properties'].get('left_over', DEFAULT_LEFT_OVER)
    config['properties']['max_retries'] = config['properties'].get('max_retries', DEFAULT_MAX_RETRIES)
    config['properties']['keep_work_dir'] = config['properties'].get('keep_work_dir', DEFAULT_KEEP_WORK_DIR)
    config['properties']['poll_interval'] = config['properties'].get('poll_interval', DEFAULT_POLL_INTERVAL)
    config['properties']['exp_backoff_factor'] = config['properties'].get('exp_backoff_factor', DEFAULT_EXP_BACKOFF_FACTOR)
    config['properties']['failure_threshold'] = config['properties'].get('failure_threshold', DEFAULT_FAILURE_THRESHOLD)
    config['properties']['failure_threshold_activation'] = config['properties'].get('failure_threshold_activation', DEFAULT_FAILURE_THRESHOLD_ACTIVATION)
    config['properties']['slack'] = config['properties'].get('slack', {})
    config['properties']['slack']['channel'] = config['properties']['slack'].get('channel', DEFAULT_SLACK_CHANNEL)
    config['properties']['slack']['token'] = config['properties']['slack'].get('token', DEFAULT_SLACK_TOKEN)

    for job_config in config['jobs']:
        job_config['dependencies'] = job_config.get('dependencies', DEFAULT_DEPENDENCIES)


def _merge_defaults(config):
    for job_conf in config['jobs']:
        job_conf['properties'] = {**config['properties'], **job_conf.get('properties', {})}
