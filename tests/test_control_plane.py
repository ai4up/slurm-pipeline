import uuid
import random
import json
from os import path
from unittest.mock import MagicMock, patch

import yaml

from slurm_pipeline import control_plane


def _test_config():
    test_dir = path.dirname(path.realpath(__file__))
    config_path = path.join(test_dir, 'test_config.yml')

    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def _results():
    test_dir = path.dirname(path.realpath(__file__))

    with open(path.join(test_dir, 'logs', 'succeeded-work.json'), 'r') as f:
        succeeded_work = json.load(f)

    with open(path.join(test_dir, 'logs', 'failed-work.json'), 'r') as f:
        failed_work = json.load(f)

    return succeeded_work, failed_work


def _mock_get_work_paths(*args):
    return [f'/path/to/city_{random.randint(0, 100)}']


@patch.object(control_plane.Scheduler, '_get_work_paths', _mock_get_work_paths)
@patch("slurm_pipeline.control_plane.time.sleep")
@patch("slurm_pipeline.control_plane.config.get_resource_config", side_effect=[
    {'cpus': 1, 'time': '01:00:00'},
    {'cpus': 2, 'time': '02:00:00'},
    {'cpus': 2, 'time': '02:00:00'}])
@patch("slurm_pipeline.control_plane.slurm.subprocess.run", side_effect=[
    MagicMock(**{'stdout.decode.return_value': 'COMPLETED'}),
    MagicMock(**{'stdout.decode.return_value': 'FAILED'}),
    MagicMock(**{'stdout.decode.return_value': 'FAILED'})])
@patch("slurm_pipeline.control_plane.slurm.sbatch_array")
def test_main(sbatch_mock, *args):
    job_id = uuid.uuid4()
    sbatch_mock.return_value = [f'{job_id}_0', f'{job_id}_1',  f'{job_id}_2']

    scheduler = control_plane.Scheduler(_test_config(), 'feature-engineering')
    scheduler.main()

    succeeded_work, failed_work = _results()
    sbatch_calls = sbatch_mock.call_args_list
    assert len(sbatch_calls) == 2
    assert sbatch_calls[0].kwargs['time'] == '01:00:00'
    assert sbatch_calls[0].kwargs['cpus'] == 1
    assert sbatch_calls[1].kwargs['time'] == '02:00:00'
    assert sbatch_calls[1].kwargs['cpus'] == 2

    assert succeeded_work[0]['job_id'] == f'{job_id}_0'
    assert failed_work[0]['job_id'] == f'{job_id}_1'
    assert failed_work[1]['job_id'] == f'{job_id}_2'


def test_groupby_resource_allocation():
    wp1 = control_plane.WorkPackage('/path/to/city_1', cpus=1, time='01:00:00')
    wp2 = control_plane.WorkPackage('/path/to/city_2', cpus=2, time='02:00:00')
    wp3 = control_plane.WorkPackage('/path/to/city_3', cpus=1, time='01:00:00')
    wp4 = control_plane.WorkPackage('/path/to/city_4', cpus=2, time='01:00:00')
    wp5 = control_plane.WorkPackage('/path/to/city_4', cpus=2, time='02:00:00')

    scheduler = control_plane.Scheduler(_test_config(), 'feature-engineering')
    grouped_wps = list(scheduler._groupby_resource_allocation([wp1, wp2, wp3, wp4, wp5]))

    assert len(grouped_wps) == 3
    for group in grouped_wps:
        assert all(wp.time == group[0].time for wp in group)
        assert all(wp.cpus == group[0].cpus for wp in group)
