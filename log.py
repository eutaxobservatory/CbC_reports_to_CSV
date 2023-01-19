import logging
from datetime import datetime

# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(filename='.log', level=logging.DEBUG, filemode='w')

logger = logging.getLogger('f')
logger.level = logging.DEBUG
logger.info(datetime.now())

# fh = logging.FileHandler('.0.log')
# fh.setLevel(logging.DEBUG)
# logger.addHandler(fh)