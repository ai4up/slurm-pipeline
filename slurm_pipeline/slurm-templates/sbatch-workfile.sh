#!/usr/bin/env bash

set -eo pipefail

pwd; hostname; date

SCRIPT="$1"
CONDA_ENV="$2"
WORKFILE="$3"

module load anaconda
module load jq

source deactivate
source activate "$CONDA_ENV"

if [ -n "$SLURM_ARRAY_TASK_ID" ]; then
    jq ".[${SLURM_ARRAY_TASK_ID}]" "$WORKFILE" | python -u "$SCRIPT"
else
    for params in $(jq -c '.[]' "$WORKFILE"); do
        # start and background process
        python -u "$SCRIPT" <<< $params &
    done

    FAIL=0
    # wait for all backgrounded processes to finish
    for job in $(jobs -p); do
        wait $job || let "FAIL+=1"
    done

    if [ "$FAIL" != "0" ]; then
        echo "${FAIL} work packages failed."
        exit 1
    fi
fi

# SCRIPT MUSS ACCEPT INPUT FROM STDIN LIKE:
# import json, sys
# request = json.load(sys.stdin)
# some_func(**request, fixed_params='XY')