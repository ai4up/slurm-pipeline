import json
import logging
import os

import yaml
import jsonschema

CONFIG_FILE = 'config.yml'
SRC_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.dirname(SRC_DIR)
CONFIG_PATH = os.path.join(PROJECT_DIR, CONFIG_FILE)


logger = logging.getLogger(__name__)

schema = """
type: object
properties:

    pipelines:
        type: array
        items:
            type: object
            required: [name, resource, concourses]
            properties:
                name:
                    type: string
                resource:
                    type: string
                jobs:
                    type: array
                group:
                    type: string
                concourses:
                    type: array
                properties:
                    type: object
                    properties:
                        current_builds_only:
                            type: boolean

    concourses:
        type: array
        items:
            type: object
            required: [name, username, password, url]
            properties:
                name:
                    type: string
                username:
                    type: string
                password:
                    type: string
                url:
                    type: string
                landscape_git_url:
                    type: string

    repo:
        type: object
        required: [git_url]
        properties:
            git_url:
                type: string
            branch:
                type: string

    commit:
        type: object
        required: [story_pattern]
        properties:
            authors:
                type: array
            msg_pattern:
                type: string
            story_pattern:
                type: string
            last_n_commits:
                type: integer
            merge_commits_only:
                type: boolean

    properties:
        type: object
        properties:
            log_level:
                type: string
                enum:
                - DEBUG
                - INFO
                - WARN
                - ERROR
            enable_build_grouping:
                type: boolean
            enable_concurrency:
                type: boolean
            experimental_enable_concurrent_logging:
                type: boolean
            polling_interval_min:
                type: number
                minimum: 1
                maximum: 60
            timeout_sec:
                type: number
                minimum: 10
"""


class UsageError(Exception):
    pass


def load():
    config = _load_config_yaml()
    # _validate(config)

    logger.debug('Successfully interpolated config:\n' + json.dumps(config, indent=2))
    return config


def _load_config_yaml():
    with open(CONFIG_PATH, 'r') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise UsageError(f'Error loading config {CONFIG_PATH}:\n{e}')


def _validate(config):
    jsonschema.validate(config, yaml.safe_load(schema))


def get_resource_config(base_path, job_config):
    default_resource_config = job_config['resources']

    for c in job_config['special_cases']:

        if file_config := c.get('file'):
            path = f"{base_path}_{file_config['type']}"
            size = os.path.getsize(path)
            min = file_config.get('file_size_min', 0)
            max = file_config.get('file_size_max', float('inf'))

            if size >= min and size <= max:
                return {**default_resource_config, **c['resources']}

    return default_resource_config