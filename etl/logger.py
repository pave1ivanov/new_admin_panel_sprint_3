import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

dotenv_path = os.path.abspath(os.path.dirname(__file__) + '/../config/.env')
load_dotenv(dotenv_path)

logging_level = {
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
}

logger = logging.getLogger('etl_application')
logger.setLevel(logging_level[os.environ.get('ETL_LOGGING_LVL')])

if not os.path.exists("logs"):
    os.makedirs("logs")
fh = RotatingFileHandler(os.path.dirname(__file__) + '/logs/etl_logs.log', maxBytes=20_000_000, backupCount=5)
formatter = logging.Formatter(
    '%(asctime)s %(levelname)-8s [%(filename)-16s:%(lineno)-5d] %(message)s'
)
fh.setFormatter(formatter)
logger.addHandler(fh)
