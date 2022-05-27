#!/usr/bin/env bash

set -eo pipefail

pwd; hostname; date

SCRIPT="$1"
CONDA_ENV="$2"


module load anaconda

source activate "$CONDA_ENV"

python -u "$SCRIPT"
