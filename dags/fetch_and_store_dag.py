from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
import hashlib
import json
import os
from minio import Minio
from minio.error import S3Error

# MinIO connection details
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minioserver:9000')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'mediae')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=AWS_ACCESS_KEY_ID,
    secret_key=AWS_SECRET_ACCESS_KEY,
    secure=False
)

logger = logging.getLogger(__name__)

def fetch_data_from_api():
    logger.info("Fetching data from API...")
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&timezone=Europe/Paris&rows=2000"
    response = requests.get(url, headers={"accept": "application/json"})
    data = response.json()

    DL_DIR = '/tmp'
    DL_FILE = os.path.join(DL_DIR, 'data.json')
    MD5_FILE = os.path.join(DL_DIR, 'data.md5')

    if not os.path.exists(DL_DIR):
        logger.info("Creating directory: %s", DL_DIR)
        os.makedirs(DL_DIR)

    logger.info("Saving data to file: %s", DL_FILE)
    with open(DL_FILE, 'w') as file:
        json.dump(data, file)

    current_md5 = hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()
    logger.info("Saving MD5 hash to file: %s", MD5_FILE)
    with open(MD5_FILE, 'w') as file:
        file.write(current_md5)

    logger.info("Data fetched and saved successfully.")
    return DL_FILE, MD5_FILE

def upload_to_minio(file_path, md5_path):
    logger.info("Uploading files to MinIO...")
    try:
        minio_client.fput_object(MINIO_BUCKET, 'data.json', file_path)
        minio_client.fput_object(MINIO_BUCKET, 'data.md5', md5_path)
        logger.info("Files uploaded successfully.")
    except S3Error as e:
        logger.error("Failed to upload files to MinIO: %s", e)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_upload_task():
    file_path, md5_path = fetch_data_from_api()
    upload_to_minio(file_path, md5_path)

with DAG(
    'fetch_and_store_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False
) as dag:
    
    fetch_and_upload = PythonOperator(
        task_id='fetch_and_upload_task',
        python_callable=fetch_and_upload_task
    )

    fetch_and_upload