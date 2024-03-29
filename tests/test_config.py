from os import path

import yaml
import pytest
from unittest.mock import patch

from slurm_pipeline import config
from slurm_pipeline.config import UsageError


def _test_config(override_conf={}):
    test_dir = path.dirname(path.realpath(__file__))
    conf_path = path.join(test_dir, 'test_config.yml')

    with open(conf_path, 'r') as f:
        conf = yaml.safe_load(f)

    return {**conf, **override_conf}


@patch('slurm_pipeline.config._load_yaml_file', return_value=_test_config({'properties': {}}))
def test_load_properties_validation(mock):
    with pytest.raises(UsageError) as exc:
        config.load('some-path')

    assert 'The conda_env must be specified' in str(exc.value)


@patch('slurm_pipeline.config._load_yaml_file', return_value=_test_config({'properties': {'slack': {}}}))
def test_load_schema_validation(mock):
    with pytest.raises(UsageError) as exc:
        config.load('some-path')

    assert "'channel' is a required property" in str(exc.value)
