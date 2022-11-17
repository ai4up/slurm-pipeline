import os
import json
from enum import Enum
from pathlib import Path
from collections import Counter

import typer
import pandas as pd
import tabulate

from slurm_pipeline import slurm
from slurm_pipeline.slurm import SlurmConfig

STATE_FILE = os.path.expanduser(os.path.join('~', '.slurm-pipeline'))
app = typer.Typer()
Status = Enum('STATUS', 'PENDING FAILED SUCCEEDED')

@app.command()
def start(
    config: str = typer.Argument(..., help='Path to slurm-config.yml file.'),
    account: str = typer.Option('eubucco', '--account', '-a', help='Slurm account to schedule tasks.'),
    log_dir: str = typer.Option('/p/projects/eubucco/logs/control_plane', '--log-dir', '-l', help='Directory to store logs.'),
    env: str = typer.Option('/home/floriann/.conda/envs/slurm-pipeline', '--env', '-e', help='Conda environment.'),
):
    """
    Start the slurm pipeline.
    """
    slurm_conf = SlurmConfig(
        cpus=1,
        partition='io',
        account=account,
        log_dir=log_dir,
        error='control_plane.stderr',
        output='control_plane.stdout',
        env_vars='PYTHONPATH=/p/projects/eubucco/slurm-pipeline',
    )
    job_id = slurm.sbatch(
        # script=os.path.join(Path(__file__).parent.absolute(), 'slurm-pipeline', 'main.py'),
        script='slurm-pipeline.main',
        conda_env=env,
        slurm_conf=slurm_conf,
        sbatch_script=os.path.join(slurm.TEMPLATE_PATH, 'sbatch.sh'),
        args=config
        )

    typer.echo(f'Control plane started. Slurm job id: {job_id}')
    _persist_cli_state({'config': config, 'job_id': job_id})


@app.command()
def abort(
    job: str = typer.Option(None, '--job', '-j', help='Name of job to abort.'),
    all: bool = typer.Option(False, '--all', help='Stop control plane and all scheduled jobs.'),
):
    """
    Stops scheduled slurm jobs.
    """
    state = _work_state()

    if all:
        for job_state in state.values():
            for wp in job_state:
                slurm.cancel(wp['job_id'])

        cp_job_id = _cli_state()['job_id']
        slurm.cancel(cp_job_id)
        typer.echo('Control plane and all scheduled jobs have been aborted.')

    elif job:
        try:
            job_state = state[job]
            for wp in job_state:
                slurm.cancel(wp['job_id'])

            typer.echo(f'{job} jobs have been aborted.')
        except KeyError:
            typer.echo(f'Error. {job} job is unknown.')

    else:
        typer.echo('Please provide either -j/--job or --all.')


@app.command()
def status():
    """
    Show number of pending, succeeded, and failed work packages.
    """
    for job, state in _work_state().items():
        typer.echo(f'----- JOB {job.upper()} -----')
        typer.echo(f'PENDING: {len(_pending(state))}')
        typer.echo(f'SUCCEEDED: {len(_succeeded(state))}')
        typer.echo(f'FAILED: {len(_failed(state))}')


@app.command()
def errors(
    n: int = typer.Option(5, '-n', help='Show most frequent n errors.'),
):
    """
    Show most frequent error types.
    """
    for job, state in _work_state().items():
        counter = _error_counter(state)
        typer.echo(f'----- JOB {job.upper()} -----')
        for error_type, count in counter.most_common(n):
            typer.echo(f'Error {error_type}: {count}')


@app.command()
def inspect_validate_ids_results(
    n: int = typer.Option(5, '-n', help='Show n cities with most errors.'),
):
    """
    Inspect and aggregate eubucco's validate ids step by country.
    """
    state = _work_state()
    job_state = state.get('validate_ids')

    results = []
    msg_on_interest = ['Nb duplicates id geom', 'Nb duplicates id attrib', 'Nb duplicates id after merge', 'Nb disagreements id_source']
    for wp in job_state:
        result = {}
        log = _stdout_log(wp)
        result['log'] = log
        result['status'] = wp['status']
        result['country'] = wp['params']['city_path'].split('/')[6]
        result['city'] = wp['params']['city_path'].split('/')[-1]
        result['error'] = _error_type(wp)

        # extract integer count from all messages on interest
        for s in msg_on_interest:
            parsed_log = log.split(s)
            if len(parsed_log) > 1:
                result[s] = int(parsed_log[1].splitlines()[0].strip())
            else:
                result[s] = 0

        results.append(result)

    df = pd.DataFrame(results)
    val_checks_by_country = df.groupby('country')[msg_on_interest].sum(numeric_only=True)

    typer.echo('Aggregated validation results for each country:')
    typer.echo(tabulate.tabulate(val_checks_by_country, headers='keys', showindex=True, tablefmt='psql'))

    typer.echo(f'The {n} cities with the most errors for each country:')
    for country, group in df.groupby('country'):
        cities_with_errors = group.set_index('city')[msg_on_interest].mean(numeric_only=True).sort_values()
        cities_with_errors = cities_with_errors[cities_with_errors > 0]
        if not cities_with_errors.empty:
            typer.echo(f'{country}: {cities_with_errors.tail(n).index}')

    typer.echo('Most common error for each country:')
    errors_by_country = df.groupby('country')['error'].value_counts().nlargest(10)
    errors_by_country = errors_by_country.rename('count').to_frame().reset_index()
    typer.echo(tabulate.tabulate(errors_by_country, headers='keys', showindex=False, tablefmt='psql'))


def _load(path):
    with open(path) as f:
        return json.load(f)


def _persist_cli_state(state):
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, sort_keys=True, indent=4, ensure_ascii=False)


def _cli_state():
    return _load(STATE_FILE)


def _config():
    config_path = _cli_state().get('config')
    return _load(config_path)


def _newest_folder(path):
    return max(Path(path).glob('*/'), key=os.path.getmtime)


def _work_state():
    state = {}
    for job in _config()['jobs']:
        path = os.path.join(_newest_folder(job['log_dir']), 'work.json')
        state[job['name']] = _load(path)
    return state


def _pending(work_packages):
    return [wp for wp in work_packages if wp['status'] == Status.PENDING.name]


def _succeeded(work_packages):
    return [wp for wp in work_packages if wp['status'] == Status.SUCCEEDED.name]


def _failed(work_packages):
    return [wp for wp in work_packages if wp['status'] == Status.FAILED.name]


def _error_counter(work_packages):
    return Counter([_error_type(wp) for wp in work_packages])


def _error_type(wp):
    return (wp['error_msg'] or '').split(':')[0] or None


def _stdout_log(wp):
    try:
        return Path(wp['stdout_log']).read_text()
    except (FileNotFoundError, TypeError):
        return ''


if __name__ == '__main__':
    app()
