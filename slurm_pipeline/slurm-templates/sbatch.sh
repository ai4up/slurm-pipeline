#!/usr/bin/env bash

set -eo pipefail

pwd; hostname; date

CONDA_ENV="$1"
SCRIPT="$2"
ARGS="$3"

module load anaconda

source deactivate
source activate "$CONDA_ENV"

if [ "$SCRIPT" == "*.py" ]; then
    # run as Python script
    python -u "$SCRIPT" "$ARGS"
else
    # run as Python module (exporting corresponding PYTHONPATH might be required)
    python -m "$SCRIPT" "$ARGS"
fi
