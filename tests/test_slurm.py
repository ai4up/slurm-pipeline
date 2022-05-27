from unittest.mock import MagicMock, patch

import pytest

from slurm_pipeline import slurm


@patch("slurm_pipeline.slurm.subprocess.run", return_value=MagicMock(**{"stdout.decode.return_value": 'PENDING'}))
def test_get_data_valid(_):
    status = slurm.status('some-job-id')

    assert status == slurm.Status.PENDING


@patch("slurm_pipeline.slurm.subprocess.run", side_effect=Exception("foobar"))
def test_get_data_invalid(_):
    with pytest.raises(Exception) as exc:
        slurm.status('invalid-job-id')
        assert "foobar" in str(exc.value)
