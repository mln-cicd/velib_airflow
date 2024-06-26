from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
import json
import os
from minio import Minio
from minio.error import S3Error

# Database connection details
DATABASE_URI = 'postgresql+psycopg2://velib_user:velib_password@user-postgres/velib'

# MinIO connection details
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minioserver:9000')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'mediae')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize Minio client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=AWS_ACCESS_KEY_ID,
    secret_key=AWS_SECRET_ACCESS_KEY,
    secure=False
)

# Define the base class for the ORM models
Base = declarative_base()

# Define the Station ORM model
class Station(Base):
    __tablename__ = 'stations'
    record_timestamp = Column(String, primary_key=True)
    stationcode = Column(String, primary_key=True)
    ebike = Column(Integer)
    mechanical = Column(Integer)
    duedate = Column(String)
    numbikesavailable = Column(Integer)
    numdocksavailable = Column(Integer)
    capacity = Column(Integer)
    is_renting = Column(String)
    is_installed = Column(String)
    is_returning = Column(String)

# Define the database models and functions directly in the DAG file
def get_db_session():
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

logger = logging.getLogger(__name__)

def sense_json_files():
    logger.info("Checking for JSON files in MinIO bucket...")
    objects = list(minio_client.list_objects(MINIO_BUCKET, recursive=True))
    json_files = [obj.object_name for obj in objects if obj.object_name.endswith('.json')]
    
    if len(json_files) > 1:
        logger.info("Found %d JSON files in the bucket.", len(json_files))
        return "fetch_json_files_task"
    else:
        logger.info("No new JSON files found in the bucket.")
        return "no_change_dummy_task"

def fetch_json_files():
    logger.info("Fetching JSON files from MinIO bucket...")
    objects = list(minio_client.list_objects(MINIO_BUCKET, recursive=True))
    json_files = [obj.object_name for obj in objects if obj.object_name.endswith('.json')]
    json_files.sort()  # Sort the files to process them in order
    
    if len(json_files) > 1:
        json_files = json_files[:-1]  # Exclude the last file
    
    logger.info("Fetched JSON files: %s", json_files)
    return json_files

def process_data(**kwargs):
    ti = kwargs['ti']
    json_files = ti.xcom_pull(task_ids='fetch_json_files_task')
    processed_data = []
    for json_file in json_files:
        logger.info("Processing JSON file: %s", json_file)
        json_obj = minio_client.get_object(MINIO_BUCKET, json_file)
        try:
            data = json.load(json_obj)
            logger.info("Processing data...")
            records = data.get("records", [])
            for record in records:
                fields = record.get("fields", {})
                processed_data.append({
                    "record_timestamp": record.get("record_timestamp", ""),
                    "stationcode": fields.get("stationcode", ""),
                    "ebike": fields.get("ebike", 0),
                    "mechanical": fields.get("mechanical", 0),
                    "duedate": fields.get("duedate", ""),
                    "numbikesavailable": fields.get("numbikesavailable", 0),
                    "numdocksavailable": fields.get("numdocksavailable", 0),
                    "capacity": fields.get("capacity", 0),
                    "is_renting": fields.get("is_renting", ""),
                    "is_installed": fields.get("is_installed", ""),
                    "is_returning": fields.get("is_returning", "")
                })
        except json.decoder.JSONDecodeError as e:
            logger.error("Error decoding JSON file %s: %s", json_file, str(e))
            # Handle the error, e.g., skip the file or perform additional processing
    logger.info("Data processing completed. Processed %d records.", len(processed_data))
    ti.xcom_push(key='processed_data', value=processed_data)  # Push processed data to XCom


def populate_stations(**kwargs):
    ti = kwargs['ti']
    processed_data = ti.xcom_pull(task_ids='process_data_task', key='processed_data')
    logger.info("Populating stations...")
    session = get_db_session()
    for record in processed_data:
        logger.debug("Processing record: %s", record)
        station = Station(
            record_timestamp=record['record_timestamp'],
            stationcode=record['stationcode'],
            ebike=record['ebike'],
            mechanical=record['mechanical'],
            duedate=record['duedate'],
            numbikesavailable=record['numbikesavailable'],
            numdocksavailable=record['numdocksavailable'],
            capacity=record['capacity'],
            is_renting=record['is_renting'],
            is_installed=record['is_installed'],
            is_returning=record['is_returning']
        )
        session.add(station)
        logger.debug("Added station: %s", station)
    session.commit()
    logger.info("Stations populated successfully.")


def delete_processed_files(**kwargs):
    ti = kwargs['ti']
    json_files = ti.xcom_pull(task_ids='fetch_json_files_task')
    for json_file in json_files:
        logger.info("Deleting processed JSON file: %s", json_file)
        minio_client.remove_object(MINIO_BUCKET, json_file)

def check_new_rows():
    logger.info("Checking for new rows in the database...")
    session = get_db_session()
    latest_record = session.query(Station).order_by(Station.record_timestamp.desc()).first()
    if latest_record:
        logger.info("Latest record timestamp: %s", latest_record.record_timestamp)
    else:
        logger.info("No records found in the database.")
    session.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,  # Set retries to 0 to fail immediately
    'retry_delay': timedelta(seconds=420)  # Set retry delay to 20 seconds
}

with DAG(
    'json2sql_dag',
    default_args=default_args,
    schedule_interval=timedelta(seconds=120),
    catchup=False
) as dag:
    
    sense_json_files_task = BranchPythonOperator(
        task_id='sense_json_files_task',
        python_callable=sense_json_files
    )

    fetch_json_files_task = PythonOperator(
        task_id='fetch_json_files_task',
        python_callable=fetch_json_files
    )

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
        provide_context=True
    )

    populate_stations_task = PythonOperator(
        task_id='populate_stations_task',
        python_callable=populate_stations,
        provide_context=True
    )

    delete_processed_files_task = PythonOperator(
        task_id='delete_processed_files_task',
        python_callable=delete_processed_files,
        provide_context=True
    )

    check_new_rows_task = PythonOperator(
        task_id='check_new_rows_task',
        python_callable=check_new_rows
    )

    no_change_dummy_task = DummyOperator(task_id='no_change_dummy_task')

sense_json_files_task >> [fetch_json_files_task, no_change_dummy_task]
fetch_json_files_task >> process_data_task >> populate_stations_task >> check_new_rows_task >> delete_processed_files_task
