#!/usr/bin/env bash

set -eo pipefail

pwd; hostname; date

SCRIPT="$1"
CONDA_ENV="$2"
WORKFILE="$3"

CITY_IDX=$(($SLURM_ARRAY_TASK_ID + 1))
CITY_PATH=$(sed -n ${CITY_IDX}p "$WORKFILE")

module load anaconda
module load jq

source activate "$CONDA_ENV"

jq ".[${CITY_IDX}]" "$WORKFILE" | python -u "$SCRIPT"

# SCRIPT MUSS EXCEPT INPUT FROM STDIN LIKE:
#  import json, sys
#  request = json.load( sys.stdin )
