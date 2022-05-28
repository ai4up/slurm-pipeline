from unittest.mock import MagicMock, patch

import pytest

from slurm_pipeline import slurm


@patch("slurm_pipeline.slurm.subprocess.run", return_value=MagicMock(**{"stdout.decode.return_value": 'PENDING', 'returncode': 0}))
def test_status(_):
    status = slurm.status('some-job-id')

    assert status == slurm.Status.PENDING


@patch("slurm_pipeline.slurm.subprocess.run", return_value=MagicMock(**{"stderr.decode.return_value": 'some-error', 'returncode': 1}))
def test_status_invalid(_):
    with pytest.raises(Exception) as exc:
        slurm.status('invalid-job-id')
        assert "some-error" in str(exc.value)
