import os
import sys
import pathlib

PROJECT_SRC_PATH = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), 'slurm_pipeline')
sys.path.append(PROJECT_SRC_PATH)