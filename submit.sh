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
#SBATCH --workdir=/p/tmp/floriann/control_plane

pwd; hostname; date

module load anaconda

source activate /home/floriann/.conda/envs/slurm-pipeline

python -u /p/projects/eubucco/slurm-pipeline/slurm_pipeline/main.py config.yml
