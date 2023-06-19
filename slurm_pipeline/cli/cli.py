import os
import re
import json
from enum import Enum
from pathlib import Path
from collections import Counter

import yaml
import typer
import pandas as pd
import tabulate
from rich.console import Console
import questionary

from slurm_pipeline import slurm
import slurm_pipeline.config as pipeline_config
from slurm_pipeline.slurm import SlurmConfig

STATE_FILE = os.path.expanduser(os.path.join('~', '.slurm-pipeline'))
PROJECT_PATH = Path(__file__).parent.parent.parent.resolve()

app = typer.Typer()
Status = Enum('STATUS', 'PENDING FAILED SUCCEEDED')


def main():
    app()


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
    stderr = 'control_plane.stderr'
    stdout = 'control_plane.stdout'

    slurm_conf = SlurmConfig(
        job_name='control_plane',
        cpus=1,
        partition='io',
        time=None,
        account=account,
        log_dir=log_dir,
        error=stderr,
        output=stdout,
        env_vars=f'PYTHONPATH={PROJECT_PATH}',
    )
    job_id = slurm.sbatch(
        script='slurm_pipeline.main',
        conda_env=env,
        slurm_conf=slurm_conf,
        args=config
        )

    typer.echo(f'Pipeline control plane started. Slurm job id: {job_id}')
    state = {
        'config': config,
        'job_id': job_id,
        'account': account,
        'stdout':  os.path.join(log_dir, stdout),
        'stderr':  os.path.join(log_dir, stderr),
    }
    _persist_cli_state(state)


@app.command()
def retry(
    dry_run: bool = typer.Option(False, '--dry-run', help='Only create new param files and slurm config, but do not restart the pipeline.'),
    account: str = typer.Option('eubucco', '--account', '-a', help='Slurm account to schedule tasks.'),
    log_dir: str = typer.Option('/p/projects/eubucco/logs/control_plane', '--log-dir', '-l', help='Directory to store logs.'),
    env: str = typer.Option('/home/floriann/.conda/envs/slurm-pipeline', '--env', '-e', help='Conda environment.'),
):
    """
    Retry failed work packages of last slurm pipeline run.
    """
    conf = _create_retry_slurm_config()
    typer.echo(f'New slurm config with updated param files has been created: {conf}')

    if not dry_run:
        start(conf, account, log_dir, env)


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
                if wp['job_id']:
                    slurm.cancel(wp['job_id'])
                else:
                    typer.echo('Not all work packages have been initialized. Please retry in a few moments.')

        cp_job_id = _cli_state()['job_id']
        slurm.cancel(cp_job_id)
        typer.echo('Control plane and all scheduled jobs have been aborted.')

    elif job:
        try:
            job_state = state[job]
            for wp in job_state:
                if wp['job_id']:
                    slurm.cancel(wp['job_id'])
                else:
                    typer.echo('Not all work packages have been initialized. Please retry in a few moments.')

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
def work(
    job: str = typer.Argument(..., help='Job name.'),
):
    """
    Show state of work packages.
    """
    state = _work_state()
    console = Console()
    with console.pager():
        json_job_state = json.dumps(state[job], indent=2, ensure_ascii=False)
        console.print(json_job_state)


@app.command()
def squeue():
    account = _cli_state().get('account')
    status = slurm.squeue(account=account)
    typer.echo(status)


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
def stdout(
    job: str = typer.Option(None, '--job', '-j', help='Job name (optionally with index, i.e. name.index).'),
    job_id: str = typer.Option(None, '--job-id', '-i', help='Job id.'),
    regex: str = typer.Option(0, '--params', '-p', help='Regex pattern to search through job params. Displays first match.'),
    failed_only: bool = typer.Option(False, '--failed', '-f', help='Show logs for failed work packages only.'),
    control_plane: bool = typer.Option(False, '--control', '-c', help='Show control plane logs.'),
):
    """
    Show stdout log for work packages.
    """
    if control_plane:
        _control_plane_logs(stderr=False)
    else:
        _logs(job_id, job, regex, failed_only, stderr=False)


@app.command()
def stderr(
    job: str = typer.Option(None, '--job', '-j', help='Job name (optionally with index, i.e. name.index).'),
    job_id: str = typer.Option(None, '--job-id', '-i', help='Job id.'),
    regex: str = typer.Option(0, '--params', '-p', help='Regex pattern to search through job params. Displays first match.'),
    failed_only: bool = typer.Option(False, '--failed', '-f', help='Show logs for failed work packages only.'),
    control_plane: bool = typer.Option(False, '--control', '-c', help='Show control plane logs.'),
):
    """
    Show stderr log for work packages.
    """
    if control_plane:
        _control_plane_logs(stderr=True)
    else:
        _logs(job_id, job, regex, failed_only, stderr=True)


def _logs(job_id=None, job=None, regex=None, failed_only=False, stderr=True):
    state = _work_state()

    try:
        if job_id:
            wp = _get_wp(state, job_id)
        elif job:
            if '.' in job:
                job, idx = job.split('.', 1)
                wp = state[job][int(idx)]
            else:
                wp = _select_job(state, failed_only, job)
        elif regex:
            p = re.compile(regex)
            wp = next(wp for job_state in state.values() for wp in job_state for param in wp['params'].values() if p.match(str(param)))
        else:
            wp = _select_job(state, failed_only)

        log_type = 'stderr' if stderr else 'stdout'
        log = _read_log(wp[log_type])

        if not log:
            typer.echo(f"Log file for job {wp['job_id']} is empty or does not yet exist.")
            return

        console = Console()
        with console.pager():
            console.print(log)

    except StopIteration:
            typer.echo('Could not find work package for given options.')

    except IndexError:
            typer.echo(f'Could not find work package for given options. Job index {idx} is out of bounds.')


def _control_plane_logs(stderr=True):
    log_type = 'stderr' if stderr else 'stdout'
    log_path = _cli_state().get(log_type)
    log = _read_log(log_path)

    console = Console()
    with console.pager():
        console.print(log)


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
        log = _read_log(wp['stdout'])
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
    return pipeline_config.load(config_path)


def _newest_job_folder(path, job_name):
    return max(Path(path).glob(f'{job_name}-*/'), key=os.path.getmtime)


def _work_state():
    state = {}
    for job in _config()['jobs']:
        path = os.path.join(_newest_job_folder(job['log_dir'], job['name']), 'work.json')
        state[job['name']] = _load(path)
    return state


def _get_wp(state, job_id):
    wp = next(wp for job_state in state.values() for wp in job_state if wp['job_id'] == job_id)
    return wp


def _select_job(state, failed_only=False, job_name=None):
    if job_name:
        wp_choices = {f"Job: {job_name} #{idx} (Slurm id: {wp['job_id']})": wp['job_id'] for idx, wp in enumerate(state[job_name])}
    else:
        wp_choices = {f"Job: {job_name} #{idx} (Slurm id: {wp['job_id']})": wp['job_id']
                      for idx, (job_name, job_state) in enumerate(state.items())
                      for wp in job_state
                      if not failed_only or wp['status'] == Status.FAILED.name}

    l = len(wp_choices.keys())
    if l > 1:
        answer = questionary.select('Please select work package:', choices=wp_choices.keys()).ask()
        job_id = wp_choices[answer]
    elif l == 1:
        job_id = list(wp_choices.values())[0]
    else:
        raise StopIteration()

    return _get_wp(state, job_id)


def _create_retry_slurm_config():
    conf = _config()
    retry_conf = _create_retry_param_files(conf)

    old_conf_path = _cli_state().get('config')
    new_conf_path = _postfix_filename(old_conf_path, '-retry')

    with open(new_conf_path, 'w') as f:
        yaml.dump(retry_conf, f, default_flow_style=False)

    return new_conf_path


def _create_retry_param_files(conf):
    for job, state in _work_state().items():
        job_conf = pipeline_config.get_job_config(conf, job)

        # create new param file for previously failed jobs
        params_path = os.path.join(_newest_job_folder(job_conf['log_dir'], job), 'params-retry.json')
        with open(params_path, 'w', encoding='utf8') as f:
            params = [wp['params'] for wp in _failed(state)]
            json.dump(params, f, indent=2, ensure_ascii=False)

        # update slurm config with new param file
        job_conf['param_files'] = [params_path]

    return conf


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


def _postfix_filename(file_path, postfix):
    name, ext = os.path.splitext(file_path)
    return f'{name}{postfix}{ext}'


def _read_log(path):
    try:
        return Path(path).read_text()
    except (FileNotFoundError, TypeError):
        return ''


if __name__ == '__main__':
    main()
