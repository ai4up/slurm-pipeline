import uuid
import random
import json
from os import path
from pathlib import Path
from unittest.mock import patch


from slurm_pipeline import control_plane
from slurm_pipeline import config


def _test_job_config():
    test_dir = path.dirname(path.realpath(__file__))
    conf_path = path.join(test_dir, 'test_config.yml')
    return config.load(conf_path)['jobs'][0]


def _newest_folder(dir_path):
    return max(Path(dir_path).glob('*/'), key=path.getmtime)


def _results():
    test_dir = path.dirname(path.realpath(__file__))
    job_log_dir = _newest_folder(path.join(test_dir, 'logs'))

    with open(path.join(job_log_dir, 'succeeded-work.json'), 'r') as f:
        succeeded_work = json.load(f)

    with open(path.join(job_log_dir, 'failed-work.json'), 'r') as f:
        failed_work = json.load(f)

    return succeeded_work, failed_work


def _mock_get_work_params(*args):
    return [{'city': f'city_{random.randint(0, 100)}'}]


@patch.object(control_plane.Scheduler, '_get_work_params', _mock_get_work_params)
@patch.object(control_plane.Scheduler, '_persist_workfile')
@patch("slurm_pipeline.control_plane.time.sleep")
@patch("slurm_pipeline.control_plane.config.get_resource_config", side_effect=[
    {'cpus': 1, 'time': '01:00:00'},
    {'cpus': 2, 'time': '02:00:00'},
    {'cpus': 2, 'time': '02:00:00'}])
@patch("slurm_pipeline.control_plane.slurm.status", side_effect=[
    control_plane.slurm.Status.COMPLETED,
    control_plane.slurm.Status.FAILED,
    control_plane.slurm.Status.FAILED,
    ])
@patch("slurm_pipeline.control_plane.slurm.sbatch_workfile")
def test_main(sbatch_mock, *args):
    sbatch_mock.return_value = 'some-job_id', []

    scheduler = control_plane.Scheduler(_test_job_config())
    scheduler.main()

    sbatch_calls = sbatch_mock.call_args_list
    assert len(sbatch_calls) == 2
    assert sbatch_calls[0].kwargs['slurm_conf'].time == '01:00:00'
    assert sbatch_calls[0].kwargs['slurm_conf'].cpus == 1
    assert sbatch_calls[1].kwargs['slurm_conf'].time == '02:00:00'
    assert sbatch_calls[1].kwargs['slurm_conf'].cpus == 2


@patch.object(control_plane.Scheduler, '_get_work_params', _mock_get_work_params)
@patch.object(control_plane.Scheduler, '_persist_workfile')
@patch("slurm_pipeline.control_plane.time.sleep")
@patch("slurm_pipeline.control_plane.config.get_resource_config", side_effect=[
    {'cpus': 1, 'time': '01:00:00', 'partition': 'io'},
    {'cpus': 2, 'time': '02:00:00', 'partition': 'io'},
    {'cpus': 2, 'time': '02:00:00', 'partition': 'io'}])
@patch("slurm_pipeline.control_plane.slurm.status", side_effect=[
    control_plane.slurm.Status.COMPLETED,
    control_plane.slurm.Status.FAILED,
    control_plane.slurm.Status.FAILED,
    ])
@patch("slurm_pipeline.control_plane.slurm.sbatch_workfile")
def test_io_scheduling(sbatch_mock, *args):
    job_id = str(uuid.uuid4())
    sbatch_mock.return_value = job_id, []

    scheduler = control_plane.Scheduler(_test_job_config())
    scheduler.main()

    succeeded_work, failed_work = _results()
    sbatch_calls = sbatch_mock.call_args_list
    assert len(sbatch_calls) == 2
    assert sbatch_calls[0].kwargs['slurm_conf'].time == '01:00:00'
    assert sbatch_calls[0].kwargs['slurm_conf'].cpus == 1
    assert sbatch_calls[1].kwargs['slurm_conf'].time == '02:00:00'
    assert sbatch_calls[1].kwargs['slurm_conf'].cpus == 2

    assert succeeded_work[0]['job_id'] == job_id
    assert failed_work[0]['job_id'] == job_id
    assert failed_work[1]['job_id'] == job_id
    assert f'{job_id}_0' in succeeded_work[0]['stderr_log']
    assert f'{job_id}_0' in failed_work[0]['stderr_log']
    assert f'{job_id}_1' in failed_work[1]['stderr_log']


@patch.object(control_plane.Scheduler, '_get_work_params', _mock_get_work_params)
@patch.object(control_plane.Scheduler, '_persist_workfile')
@patch("slurm_pipeline.control_plane.time.sleep")
@patch("slurm_pipeline.control_plane.config.get_resource_config", return_value={'cpus': 1, 'time': '01:00:00'})
@patch("slurm_pipeline.control_plane.slurm.status", return_value=control_plane.slurm.Status.OUT_OF_MEMORY)
@patch("slurm_pipeline.control_plane.slurm.sbatch_workfile")
def test_retry(sbatch_mock, *args):
    sbatch_mock.return_value = 'some-job_id', []

    conf = _test_job_config()
    max_retries = 3
    conf['properties']['max_retries'] = max_retries
    scheduler = control_plane.Scheduler(conf)
    scheduler.main()

    # as the resource config is identical for all work packages,
    # only one job with multiple tasks will be scheduled
    sbatch_calls = sbatch_mock.call_args_list
    assert len(sbatch_calls) == max_retries + 1


def test_groupby_resource_allocation():
    wp1 = control_plane.WorkPackage({'city': 'city_1'}, cpus=1, time='01:00:00')
    wp2 = control_plane.WorkPackage({'city': 'city_2'}, cpus=2, time='02:00:00')
    wp3 = control_plane.WorkPackage({'city': 'city_3'}, cpus=1, time='01:00:00')
    wp4 = control_plane.WorkPackage({'city': 'city_4'}, cpus=2, time='01:00:00')
    wp5 = control_plane.WorkPackage({'city': 'city_5'}, cpus=2, time='02:00:00')

    scheduler = control_plane.Scheduler(_test_job_config())
    grouped_wps = list(scheduler._groupby_resource_allocation([wp1, wp2, wp3, wp4, wp5]))

    assert len(grouped_wps) == 3
    for group in grouped_wps:
        assert all(wp.time == group[0].time for wp in group)
        assert all(wp.cpus == group[0].cpus for wp in group)


@patch.object(control_plane.Scheduler, '_duration', return_value=100)
def test_every_n_polls(mock):
    scheduler = control_plane.Scheduler(_test_job_config())

    scheduler.poll_interval = 4
    assert scheduler._every_n_polls(n=25) == True

    scheduler.poll_interval = 9
    assert scheduler._every_n_polls(n=11) == True

    scheduler.poll_interval = 9
    assert scheduler._every_n_polls(n=12) == False
