#!/usr/bin/env bash

set -eo pipefail

pwd; hostname; date

MAX_CONCURRENT_JOBS=5

CONDA_ENV="$1"
SCRIPT="$2"
WORKFILE="$3"

module load anaconda
module load jq

for i in $(seq ${CONDA_SHLVL}); do
    source deactivate
done
source activate "$CONDA_ENV"

echo "Using conda env ${CONDA_ENV} and Python version $(python --version) ($(which python))."

if [ -n "$SLURM_ARRAY_TASK_ID" ]; then
    if [ -x "$(command -v mprof)" ]; then
        echo "Profiling memory usage of Python process..."
        jq ".[${SLURM_ARRAY_TASK_ID}]" "$WORKFILE" | mprof run --output "mprofile_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.dat" "$SCRIPT"
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
        # write stdout and stderr of each process to separate log files
        if [ -x "$(command -v mprof)" ]; then
            echo "Profiling memory usage of Python process..."
            mprof run --output "mprofile_${SLURM_JOBID}_${i}.dat" "$SCRIPT" <<< $params > "${SLURM_JOBID}_${i}.stdout" 2> "${SLURM_JOBID}_${i}.stderr" || touch "${SLURM_JOBID}_${i}.failed" &
        else
            python -u "$SCRIPT" <<< $params > "${SLURM_JOBID}_${i}.stdout" 2> "${SLURM_JOBID}_${i}.stderr" || touch "${SLURM_JOBID}_${i}.failed" &
        fi

        i=$((i+1))
    done

    # wait for all backgrounded processes to finish
    wait

    # report slurm job as failed if any background process failed
    if ls *.failed &> /dev/null; then
        exit 1
    fi
fi

# SCRIPT MUSS ACCEPT INPUT FROM STDIN LIKE:
# import json, sys
# request = json.load(sys.stdin)
# some_func(**request, fixed_params='XY')