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
    jq -c '.[]' "$WORKFILE" | while read params; do
        python -u "$SCRIPT" <<< $params
    done
fi

# SCRIPT MUSS ACCEPT INPUT FROM STDIN LIKE:
# import json, sys
# request = json.load(sys.stdin)
# some_func(**request, fixed_params='XY')