from unittest.mock import MagicMock, patch

import pytest

from slurm_pipeline import slurm


@patch('slurm_pipeline.slurm.subprocess.run', return_value=MagicMock(**{'stdout.decode.return_value': 'PENDING', 'returncode': 0}))
def test_status(_):
    status = slurm.status('some-job-id')

    assert status == slurm.Status.PENDING


@patch('slurm_pipeline.slurm.subprocess.run', return_value=MagicMock(**{'stderr.decode.return_value': 'some-error', 'returncode': 1}))
def test_status_invalid(_):
    with pytest.raises(Exception) as exc:
        slurm.status('invalid-job-id')

    assert 'some-error' in str(exc.value)


@pytest.mark.parametrize('sbatch_kwargs', [{'workfile': 'some-workfile'}, {'array': '0-2'}])
def test_sbatch_inconsistent_params(sbatch_kwargs):
    with pytest.raises(Exception) as exc:
        slurm.sbatch(script='some-script',
        log_dir='some-log-dir',
        conda_env='some-conda-env',
        **sbatch_kwargs
        )

    assert 'Please pass a custom sbatch script.' in str(exc.value)


@pytest.mark.parametrize('time_str, result_days, result_sec', [
    ('1-10:00:00', 1, 36000),
    ('1-10:00', 1, 36000),
    ('1-10', 1, 36000),
    ('00:60:00', 0, 3600),
    ('5:30', 0, 330),
    ('30', 0, 1800)])
def test_parse_time(time_str, result_days, result_sec):
    t = slurm.parse_time(time_str)

    assert t.days == result_days
    assert t.seconds == result_sec
