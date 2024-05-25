# __init__.py
import os
import sys
import logging
from pathlib import Path

# Get the absolute path of the current directory
THIS_DIR = Path(__file__).resolve().parent
# Get the absolute path of the root directory (parent of 'app')
ROOT_DIR = THIS_DIR.parent
# Add the root directory to the Python module search path
sys.path.append(str(ROOT_DIR))

# Database configuration
DB_HOST = 'postgres'
DB_PORT = '5432'
DB_NAME = os.environ['POSTGRES_DB']
DB_USER = os.environ['POSTGRES_USER']
DB_PASSWORD = os.environ['POSTGRES_PASSWORD']

DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
PREFECT_API_DATABASE_CONNECTION_URL = os.environ['PREFECT_API_DATABASE_CONNECTION_URL']
DL_DIR = "records"
DL_FILE = "records/last_api_call.json"
MD5_FILE = "records/check.md5"




# Configure the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a stream handler for console output
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Create a file handler for logging to a file
file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(formatter)

# Add the handlers to the root logger
root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)
