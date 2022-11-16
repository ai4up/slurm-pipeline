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
    if [ -x "$(command -v mprof)" ]; then
        echo "Profiling memory usage of Python process..."
        jq ".[${SLURM_ARRAY_TASK_ID}]" "$WORKFILE" | mprof run --output "mprofile_${SLURM_JOBID}_${SLURM_ARRAY_TASK_ID}.dat" "$SCRIPT"
    else
        jq ".[${SLURM_ARRAY_TASK_ID}]" "$WORKFILE" | python -u "$SCRIPT"
    fi
else
    i=0
    echo "Job array not specified. Iterating over work packages in workfile concurrently (max ${MAX_CONCURRENT_JOBS} in parallel)."
    for params in $(jq -c '.[]' "$WORKFILE"); do

        # process at most 5 work packages concurrently
        until [ $(jobs -p | wc -w) -lt $MAX_CONCURRENT_JOBS ]; do
            sleep 2
        done

        # start and background process
        # write memory usage to .dat file if mprof is installed
        # write stdout and stderr of each process to separate log file
        # as tee only captures stdout, stderr and stdout are swapped with 3>&1 1>&2 2>&3
        if [ -x "$(command -v mprof)" ]; then
            echo "Profiling memory usage of Python process..."
            (mprof run --output "mprofile_${SLURM_JOBID}_${i}.dat" "$SCRIPT" <<< $params | tee "${SLURM_JOBID}_${i}.stdout") 3>&1 1>&2 2>&3 | tee "${SLURM_JOBID}_${i}.stderr" &
        else
            (python -u "$SCRIPT" <<< $params | tee "${SLURM_JOBID}_${i}.stdout") 3>&1 1>&2 2>&3 | tee "${SLURM_JOBID}_${i}.stderr" &
        fi

        ((i++))
    done

    # wait for all backgrounded processes to finish
    wait
fi

# SCRIPT MUSS ACCEPT INPUT FROM STDIN LIKE:
# import json, sys
# request = json.load(sys.stdin)
# some_func(**request, fixed_params='XY')