import logging
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from config.settings import DATABASE_URL, LOG_LEVEL

# logging.basicConfig(level=LOG_LEVEL)

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        logger.info("INTIALIZING SQLALCHEMY ENGINE...")
        _engine = create_engine(DATABASE_URL)
        _verify_connection(_engine)
        logger.info("SQLALCHEMY ENGINE INITIALIZED SUCCESSFULLY.")
    return _engine

def _verify_connection(engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("DATABASE CONNECTION VERIFIED SUCCESSFULLY.")
    except OperationalError as e:
        logger.error(
            f"DATABASE CONNECTION FAILED\n"
            f"DETAIL: {e}"
            )
        raise

def dispose_engine():
    global _engine
    if _engine is not None:
        logger.info("DISPOSING SQLALCHEMY ENGINE...")
        _engine.dispose()
        _engine = None
        logger.info("SQLALCHEMY ENGINE DISPOSED SUCCESSFULLY.")