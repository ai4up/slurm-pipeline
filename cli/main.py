import json
from enum import Enum
from pathlib import Path
from collections import Counter

import typer
import pandas as pd
import tabulate


app = typer.Typer()
Status = Enum('STATUS', 'PENDING FAILED SUCCEEDED')


@app.command()
def status(
        path: str = typer.Argument('work.json', help='Path to work.json file.'),
):
    """
    Show number of pending, succeeded, and failed work packages.
    """
    work = _load(path)

    typer.echo(f'PENDING: {len(_pending(work))}')
    typer.echo(f'SUCCEEDED: {len(_succeeded(work))}')
    typer.echo(f'FAILED: {len(_failed(work))}')


@app.command()
def errors(
    path: str = typer.Argument('work.json', help='Path to work.json file.'),
    n: int = typer.Option(5, '-n', help='Show most frequent n errors.'),
):
    """
    Show most frequent error types.
    """
    work = _load(path)

    counter = _error_counter(work)
    for error_type, count in counter.most_common(n):
        typer.echo(f'Error {error_type}: {count}')


@app.command()
def inspect_validate_ids_results(
    path: str = typer.Argument('work.json', help='Path to work.json file.'),
):
    """
    Inspect and aggregate eubucco's validate ids step by country.
    """
    work = _load(path)

    results = []
    msg_on_interest = ['Nb duplicates id geom', 'Nb duplicates id attrib', 'Nb duplicates id after merge', 'Nb disagreements id_source']
    for wp in work:
        result = {}
        log = _stdout_log(wp)
        result['log'] = log
        result['status'] = wp['status']
        result['country'] = wp['params']['city_path'].split('/')[6]
        result['city'] = wp['params']['city_path'].split('/')[-1]

        # extract integer count from all messages on interest
        for s in msg_on_interest:
            parsed_log = log.split(s)
            if len(parsed_log) > 1:
                result[s] = parsed_log[1].splitlines()[0].strip()
            else:
                result[s] = 0

        results.append(result)

    df = pd.DataFrame(results)
    errors_by_country = df.groupby('country')[msg_on_interest].sum()

    typer.echo('Aggregated validation results by country:')
    typer.echo(tabulate.tabulate(errors_by_country, headers='keys', showindex=True, tablefmt='psql'))

    typer.echo('Failed cities results by country:')
    for country, group in df.groupby('country'):
        cities_with_errors = group.set_index('city')[msg_on_interest].mean().sort_values()
        cities_with_errors = cities_with_errors[cities_with_errors > 0]
        if not cities_with_errors.empty:
            typer.echo(f'{country}: {cities_with_errors.tail(5).index}')


def _load(path):
    with open(path) as f:
        return json.load(f)


def _pending(work_packages):
    return [wp for wp in work_packages if wp['status'] == Status.PENDING.name]


def _succeeded(work_packages):
    return [wp for wp in work_packages if wp['status'] == Status.SUCCEEDED.name]


def _failed(work_packages):
    return [wp for wp in work_packages if wp['status'] == Status.FAILED.name]


def _error_counter(work_packages):
    return Counter([str(wp['error_msg']).split(':')[0] for wp in work_packages])


def _stdout_log(wp):
    if path := wp['stdout_log']:
        return Path(path).read_text()

    return ''


if __name__ == '__main__':
    app()
