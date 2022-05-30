import os
import json
import logging
import textwrap

import yaml
import jsonschema

SRC_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(SRC_DIR, '..', 'config.yml')

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_POLL_INTERVAL = 30
DEFAULT_EXP_BACKOFF_FACTOR = 4
DEFAULT_MAX_RETRIES = 3
DEFAULT_KEEP_WORK_DIR = False
DEFAULT_LEFT_OVER = None
DEFAULT_CUSTOM_WORKFILE = None
DEFAULT_SLACK_CHANNEL = None
DEFAULT_SLACK_TOKEN = None
DEFAULT_ACCOUNT = None

SCHEMA_PROPERTIES = """
properties:
    type: object
    properties:
        conda_env:
            type: string
        account:
            type: string
        log_level:
            type: string
            enum:
            - DEBUG
            - INFO
            - WARN
            - ERROR
        left_over:
            type: string
        custom_workfile:
            type: string
        keep_work_dir:
            type: boolean
        exp_backoff_factor:
            type: integer
        max_retries:
            type: integer
            minimum: 0
        poll_interval:
            type: integer
            minimum: 10
            maximum: 3600
        slack:
            type: object
            required: [channel, token]
            properties:
                channel:
                    type: string
                token:
                    type: string
"""

SCHEMA = f"""
type: object
properties:

    jobs:
        type: array
        items:
            type: object
            required: [name, script, data_dir, log_dir, resources]
            properties:
                name:
                    type: string
                script:
                    type: string
                data_dir:
                    type: string
                log_dir:
                    type: string
                resources:
                    type: object
                    required: [cpus, time]
                    properties:
                        cpus:
                            type: integer
                        time:
                            type: string
                special_cases:
                    type: array
                    items:
                        type: object
                        properties:
                            name:
                                type: string
                            resources:
                                type: object
                                properties:
                                    cpus:
                                        type: integer
                                    time:
                                        type: string
                            file:
                                type: object
                                required: [type]
                                properties:
                                    type:
                                        type: string
                                    file_size_min:
                                        type: integer
                                    file_size_max:
                                        type: integer

                {textwrap.indent(SCHEMA_PROPERTIES, ' ' * 16)}

    {textwrap.indent(SCHEMA_PROPERTIES, ' ' * 4)}
"""

logger = logging.getLogger(__name__)


class UsageError(Exception):
    pass


def load():
    config = _load_config_yaml()
    _validate(config)
    _set_defaults(config)
    _merge_defaults(config)

    logger.info('Successfully interpolated config:\n' + json.dumps(config, indent=2))
    return config


def get_job_config(config, job_name):
    return next(job_conf for job_conf in config['jobs'] if job_conf['name'] == job_name)


def get_resource_config(base_path, job_config):
    default_resource_config = job_config['resources']

    for sc in job_config['special_cases']:

        if file_config := sc.get('file'):
            path = f"{base_path}_{file_config['type']}"
            size = os.path.getsize(path)
            min = file_config.get('file_size_min', 0)
            max = file_config.get('file_size_max', float('inf'))

            if size >= min and size <= max:
                return {**default_resource_config, **sc['resources']}

    return default_resource_config


def _load_config_yaml():
    with open(CONFIG_PATH, 'r') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise UsageError(f'Error loading config {CONFIG_PATH}:\n{e}')


def _validate(config):
    jsonschema.validate(config, yaml.safe_load(SCHEMA))
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
    config['properties']['custom_workfile'] = config['properties'].get('custom_workfile', DEFAULT_CUSTOM_WORKFILE)
    config['properties']['max_retries'] = config['properties'].get('max_retries', DEFAULT_MAX_RETRIES)
    config['properties']['keep_work_dir'] = config['properties'].get('keep_work_dir', DEFAULT_KEEP_WORK_DIR)
    config['properties']['poll_interval'] = config['properties'].get('poll_interval', DEFAULT_POLL_INTERVAL)
    config['properties']['exp_backoff_factor'] = config['properties'].get('exp_backoff_factor', DEFAULT_EXP_BACKOFF_FACTOR)
    config['properties']['slack'] = config['properties'].get('slack', {})
    config['properties']['slack']['channel'] = config['properties']['slack'].get('channel', DEFAULT_SLACK_CHANNEL)
    config['properties']['slack']['token'] = config['properties']['slack'].get('token', DEFAULT_SLACK_TOKEN)


def _merge_defaults(config):
    for job_conf in config['jobs']:
        job_conf['properties'] = {**config['properties'], **job_conf.get('properties', {})}
