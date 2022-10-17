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

jq ".[${SLURM_ARRAY_TASK_ID}]" "$WORKFILE" | python -u "$SCRIPT"


# SCRIPT MUSS ACCEPT INPUT FROM STDIN LIKE:
# import json, sys
# request = json.load(sys.stdin)
# some_func(**request, fixed_params='XY')