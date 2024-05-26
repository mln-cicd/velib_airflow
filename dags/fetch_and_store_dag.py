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

def ensure_minio_bucket():
    logger.info("Ensuring MinIO bucket exists...")
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            logger.info("Bucket does not exist. Creating bucket: %s", MINIO_BUCKET)
            minio_client.make_bucket(MINIO_BUCKET)
        else:
            logger.info("Bucket already exists: %s", MINIO_BUCKET)
    except S3Error as e:
        logger.error("Failed to ensure MinIO bucket: %s", e)
        raise

def fetch_data_from_api(**kwargs):
    logger.info("Fetching data from API...")
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&timezone=Europe/Paris&rows=2000"
    try:
        response = requests.get(url, headers={"accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        logger.info("Data fetched successfully.")
        kwargs['ti'].xcom_push(key='api_data', value=data)
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch data from API: %s", e)
        raise

def write_md5(**kwargs):
    logger.info("Writing MD5 hash to file...")
    DL_DIR = '/tmp'
    MD5_FILE = os.path.join(DL_DIR, 'data.md5')
    data = kwargs['ti'].xcom_pull(key='api_data')

    if not os.path.exists(DL_DIR):
        logger.info("Creating directory: %s", DL_DIR)
        os.makedirs(DL_DIR)

    try:
        current_md5 = hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()
        with open(MD5_FILE, 'w') as file:
            file.write(current_md5)
        logger.info("MD5 hash written to file successfully.")
        kwargs['ti'].xcom_push(key='md5_file', value=MD5_FILE)
        kwargs['ti'].xcom_push(key='current_md5', value=current_md5)
    except Exception as e:
        logger.error("Failed to write MD5 hash to file: %s", e)
        raise

def write_json(**kwargs):
    logger.info("Writing JSON data to file...")
    DL_DIR = '/tmp'
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    DL_FILE = os.path.join(DL_DIR, f'data_{timestamp}.json')
    data = kwargs['ti'].xcom_pull(key='api_data')

    if not os.path.exists(DL_DIR):
        logger.info("Creating directory: %s", DL_DIR)
        os.makedirs(DL_DIR)

    try:
        with open(DL_FILE, 'w') as file:
            json.dump(data, file)
        logger.info("JSON data written to file successfully.")
        kwargs['ti'].xcom_push(key='json_file', value=DL_FILE)
    except Exception as e:
        logger.error("Failed to write JSON data to file: %s", e)
        raise

def upload_to_minio(**kwargs):
    logger.info("Uploading files to MinIO...")
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='json_file')
    md5_path = ti.xcom_pull(key='md5_file')
    current_md5 = ti.xcom_pull(key='current_md5')

    try:
        previous_md5_obj = minio_client.get_object(MINIO_BUCKET, 'previous_md5.txt')
        previous_md5 = previous_md5_obj.read().decode('utf-8').strip()
    except S3Error as e:
        if e.code == 'NoSuchKey':
            logger.warning("Previous MD5 hash file not found in MinIO. Treating as first run.")
            previous_md5 = None
        else:
            logger.error("Failed to get previous MD5 hash from MinIO: %s", e)
            raise

    if previous_md5 is None or previous_md5 != current_md5:
        logger.info("MD5 hash has changed or no previous MD5 hash found. Uploading new files.")
        try:
            minio_client.fput_object(MINIO_BUCKET, os.path.basename(file_path), file_path)
            minio_client.fput_object(MINIO_BUCKET, 'previous_md5.txt', md5_path)
            logger.info("Files uploaded successfully.")
        except S3Error as e:
            logger.error("Failed to upload files to MinIO: %s", e)
            raise
    else:
        logger.info("MD5 hash has not changed. Skipping file upload.")

def check_files_in_minio(**kwargs):
    logger.info("Checking file in MinIO...")
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='json_file')
    file_name = os.path.basename(file_path)

    try:
        minio_client.stat_object(MINIO_BUCKET, file_name)
        logger.info("File '%s' exists in MinIO bucket.", file_name)
    except S3Error as e:
        if e.code == 'NoSuchKey':
            logger.error("File '%s' does not exist in MinIO bucket.", file_name)
            raise ValueError(f"File '{file_name}' does not exist in MinIO bucket.")
        else:
            logger.error("Failed to check file in MinIO bucket: %s", e)
            raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fetch_and_store_dag',
    default_args=default_args,
    schedule_interval=timedelta(seconds=120),
    catchup=False
) as dag:
    
    ensure_bucket = PythonOperator(
        task_id='ensure_bucket_task',
        python_callable=ensure_minio_bucket
    )

    fetch_data = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_api,
        provide_context=True
    )

    write_md5_task = PythonOperator(
        task_id='write_md5_task',
        python_callable=write_md5,
        provide_context=True
    )

    write_json_task = PythonOperator(
        task_id='write_json_task',
        python_callable=write_json,
        provide_context=True
    )

    upload_to_minio_task = PythonOperator(
        task_id='upload_to_minio_task',
        python_callable=upload_to_minio,
        provide_context=True
    )

    check_files_task = PythonOperator(
        task_id='check_files_task',
        python_callable=check_files_in_minio,
        execution_timeout=timedelta(minutes=10),
        provide_context=True
    )

    ensure_bucket >> fetch_data >> [write_md5_task, write_json_task] >> upload_to_minio_task
    upload_to_minio_task >> check_files_task