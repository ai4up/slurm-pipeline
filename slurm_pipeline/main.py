import logging

import config
from control_plane import Scheduler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    conf = config.load()
    logger.setLevel(conf['properties']['log_level'])
    job_conf = config.get_job_config(conf, 'preprocessing')
    scheduler = Scheduler(job_conf)
    scheduler.main()