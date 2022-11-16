#!/usr/bin/env bash

set -eo pipefail

pwd; hostname; date

MAX_CONCURRENT_JOBS=5

SCRIPT="$1"
CONDA_ENV="$2"
WORKFILE="$3"

module load anaconda
module load jq

source deactivate
source activate "$CONDA_ENV"

if [ -n "$SLURM_ARRAY_TASK_ID" ]; then
    jq ".[${SLURM_ARRAY_TASK_ID}]" "$WORKFILE" | mprof run --output "mprofile_${SLURM_JOBID}_${SLURM_ARRAY_TASK_ID}.dat" "$SCRIPT"
else
    i=0
    for params in $(jq -c '.[]' "$WORKFILE"); do

        # process at most 5 work packages concurrently
        until [ $(jobs -p | wc -w) -lt $MAX_CONCURRENT_JOBS ]; do
            sleep 2
        done

        # start and background process; write memory usage to .dat file; write stdout and stderr to log file
        mprof run --output "mprofile_${SLURM_JOBID}_${i}.dat" "$SCRIPT" <<< $params &

        ((i++))
    done

    # wait for all backgrounded processes to finish
    wait
fi

# SCRIPT MUSS ACCEPT INPUT FROM STDIN LIKE:
# import json, sys
# request = json.load(sys.stdin)
# some_func(**request, fixed_params='XY')