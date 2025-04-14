# Slurm pipeline
The slurm pipeline facilitates the scheduling of [slurm](https://slurm.schedmd.com/overview.html) jobs. It allows to sequentially schedule multiple jobs and supports logging, retrying, dynamic allocation of computing resources, and intelligent parallization via slurm job arrays.


## Install
Install from GitHub:
```
pip install git+https://github.com/ai4up/slurm-pipeline@main
```


## CLI
A CLI allows to control the slurm pipeline, such as starting & aborting jobs, inspecting logs, and more. See [CLI docs](#cli-docs) section for more details.

<!-- agg --speed=1.5 --cols=100 --rows=25 --font-size=24 docs/slurm-pipeline-demo.cast docs/slurm-pipeline-demo.gif -->
![](./docs/slurm-pipeline-demo.gif)


**Commands**:

* `abort`: Stops scheduled slurm jobs.
* `retry`: Retry failed work packages of last slurm...
* `start`: Start the slurm pipeline.
* `status`: Show number of pending, succeeded, and failed...
* `stderr`: Show stderr log for work packages.
* `stdout`: Show stdout log for work packages.
* `work`: Show state of work packages.

**Help**:

* `--help`: Show available options and commands.


## Config file
The config file is the heart of the slurm-pipeline. Here, all jobs and properties are specified. The job order in the config determines the sequential scheduling order. 

The minimal config looks like:
```yaml
jobs:
  - name: <some-name>
    script: </path-to/script-name.py>
    param_files:
    - </path-to/workfile-name-1.py>
    - </path-to/workfile-name-2.py>
    log_dir: </path-to-log-dir/>
    resources:
      cpus: 1
      time: "06:00:00"

properties:
  conda_env: </path-to/.conda/envs/env-name>
  account: <slurm-account>
```
A more extensive template can be found at [`docs/template-config.yml`](./docs/template-config.yml). For an exact definition of the schema and all available properties, please see [`slurm_pipeline/config.py`](./slurm_pipeline/config.py).

## Jobs
Each job is based on an executable Python script. The path to the `script` must be specified in the slurm config. Additionally, a JSON, YAML, or CSV formatted `param_file` can be provided which includes a list of arguments that will be passed one by one via stdin to the Python script. For example, the following `param_file` will trigger two runs of the job script with different parameterizations:
```yaml
- param_1: some-value
  param_2: some-value

- param_1: other-value
  param_2: other-value
```
 These arguments can be used in the Python script as follows: 
```Python
#!/usr/bin/env python

import json

def job_main(param_1, param_2):
    pass

if __name__ == '__main__':
    params = json.load(sys.stdin)
    job_main(**params)
```


## Development

Build from source:
```
poetry build
pip install dist/slurm_pipeline-*.whl
```


## CLI docs


### `start`

Start the slurm pipeline.

**Usage**:

```console
$ start [OPTIONS] CONFIG
```

**Arguments**:

* `CONFIG`: Path to slurm-config.yml file.  [required]

**Options**:

* `-a, --account TEXT`: Slurm account to schedule tasks.  [default: eubucco]
* `-l, --log-dir TEXT`: Directory to store logs.  [default: /p/projects/eubucco/logs/control_plane]
* `-e, --env TEXT`: Conda environment.  [default: /home/floriann/.conda/envs/slurm-pipeline]


### `abort`

Stops scheduled slurm jobs.

**Usage**:

```console
$ abort [OPTIONS]
```

**Options**:

* `-j, --job TEXT`: Name of job to abort.
* `--all`: Stop control plane and all scheduled jobs.  [default: False]



### `status`

Show number of pending, succeeded, and failed work packages.

**Usage**:

```console
$ status [OPTIONS]
```


### `stderr`

Show stderr log for work packages.

**Usage**:

```console
$ stderr [OPTIONS]
```

**Options**:

* `-j, --job TEXT`: Job name (optionally with index, i.e. name.index).
* `-i, --job-id TEXT`: Job id.
* `-p, --params TEXT`: Regex pattern to search through job params. Displays first match.  [default: 0]

### `stdout`

Show stdout log for work packages.

**Usage**:

```console
$ stdout [OPTIONS]
```

**Options**:

* `-j, --job TEXT`: Job name (optionally with index, i.e. name.index).
* `-i, --job-id TEXT`: Job id.
* `-p, --params TEXT`: Regex pattern to search through job params. Displays first match.  [default: 0]


### `work`

Show state of work packages.

**Usage**:

```console
$ work [OPTIONS] JOB
```

**Arguments**:

* `JOB`: Job name.  [required]


### `retry`

Retry failed work packages of last slurm pipeline run.

**Usage**:

```console
$ retry [OPTIONS]
```

**Options**:

* `-a, --account TEXT`: Slurm account to schedule tasks.  [default: eubucco]
* `-l, --log-dir TEXT`: Directory to store logs.  [default: /p/projects/eubucco/logs/control_plane]
* `-e, --env TEXT`: Conda environment.  [default: /home/floriann/.conda/envs/slurm-pipeline]
