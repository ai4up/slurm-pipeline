import logging
import argparse

from slurm_pipeline import config
from slurm_pipeline.control_plane import Scheduler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    args = parser.parse_args()
    
    conf_path = args.config
    conf = config.load(conf_path)

    logger.setLevel(conf['properties']['log_level'])
    job_conf = config.get_job_config(conf, 'feature-engineering')

    scheduler = Scheduler(job_conf)
    scheduler.main()