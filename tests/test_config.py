from os import path

import yaml
import pytest
from unittest.mock import patch

from slurm_pipeline import config
from slurm_pipeline.config import UsageError

test_dir = path.dirname(path.realpath(__file__))
config.CONFIG_PATH = path.join(test_dir, 'test_config.yml')


def _test_config(override_conf={}):
    with open(config.CONFIG_PATH, 'r') as f:
        conf = yaml.safe_load(f)

    return {**conf, **override_conf}


@patch('slurm_pipeline.config._load_config_yaml', return_value=_test_config({'properties': {}}))
def test_load_properties_validation(mock):
    with pytest.raises(UsageError) as exc:
        config.load()

    assert 'The conda_env must be specified' in str(exc.value)


@patch('slurm_pipeline.config._load_config_yaml', return_value=_test_config({'properties': {'slack': {}}}))
def test_load_schema_validation(mock):
    with pytest.raises(UsageError) as exc:
        config.load()

    assert "'channel' is a required property" in str(exc.value)

