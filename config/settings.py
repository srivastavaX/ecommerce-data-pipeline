import os
from pathlib import Path
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent.parent / 'env'
load_dotenv(dotenv_path=ENV_PATH)


# DATABASE ---------
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


# API ---------
API_URL = os.getenv("API_URL")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", 10))


# PIPELINE ---------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
PIPELINE_ENV = os.getenv("PIPELINE_ENV", "development")