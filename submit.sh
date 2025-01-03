#!/bin/bash

#SBATCH --job-name=control_plane
#SBATCH --partition=io
#SBATCH --qos=io
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=control_plane.stdout
#SBATCH --error=control_plane.stderr
#SBATCH --chdir=/p/tmp/floriann/control_plane

pwd; hostname; date

module load anaconda

source activate /home/floriann/.conda/envs/slurm-pipeline

CONFIG_PATH=$1

PYTHONPATH=/p/projects/eubucco/slurm-pipeline python -m slurm_pipeline.main "$CONFIG_PATH"
