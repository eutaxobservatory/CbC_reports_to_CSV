import logging
from datetime import datetime

logging.basicConfig(filename='.log', level=logging.INFO, filemode='a')
logger = logging.getLogger()
logger.info("\n%s",datetime.now())