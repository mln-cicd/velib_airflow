from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
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

# Define the Location ORM model (assuming it exists)
class Location(Base):
    __tablename__ = 'locations'
    stationcode = Column(String, primary_key=True)
    # Add other columns as needed

# Define the database models and functions directly in the DAG file
def get_db_session():
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def sense_json_files():
    objects = list(minio_client.list_objects(MINIO_BUCKET, recursive=True))
    json_files = [obj.object_name for obj in objects if obj.object_name.endswith('.json')]
    
    if len(json_files) > 1:
        return "fetch_json_files_task"
    else:
        return "no_change_dummy_task"

def fetch_json_files():
    objects = list(minio_client.list_objects(MINIO_BUCKET, recursive=True))
    json_files = [obj.object_name for obj in objects if obj.object_name.endswith('.json')]
    json_files.sort()  # Sort the files to process them in order
    
    if len(json_files) > 1:
        json_files = json_files[:-1]  # Exclude the last file
    
    return json_files

def process_and_populate_data(**kwargs):
    ti = kwargs['ti']
    json_files = ti.xcom_pull(task_ids='fetch_json_files_task')
    session = get_db_session()
    
    for json_file in json_files:
        json_obj = minio_client.get_object(MINIO_BUCKET, json_file)
        try:
            data = json.load(json_obj)
            records = data.get("records", [])
            for record in records:
                fields = record.get("fields", {})
                stationcode = fields.get("stationcode", "")
                if stationcode:  # Ensure stationcode is not empty
                    # Check if stationcode exists in locations table
                    location_exists = session.query(Location).filter_by(stationcode=stationcode).first()
                    if location_exists:
                        station = Station(
                            record_timestamp=record.get("record_timestamp", ""),
                            stationcode=stationcode,
                            ebike=fields.get("ebike", 0),
                            mechanical=fields.get("mechanical", 0),
                            duedate=fields.get("duedate", ""),
                            numbikesavailable=fields.get("numbikesavailable", 0),
                            numdocksavailable=fields.get("numdocksavailable", 0),
                            capacity=fields.get("capacity", 0),
                            is_renting=fields.get("is_renting", ""),
                            is_installed=fields.get("is_installed", ""),
                            is_returning=fields.get("is_returning", "")
                        )
                        session.add(station)
        except json.decoder.JSONDecodeError:
            pass  # Handle the error, e.g., skip the file or perform additional processing
    
    session.commit()

def delete_processed_files(**kwargs):
    ti = kwargs['ti']
    json_files = ti.xcom_pull(task_ids='fetch_json_files_task')
    for json_file in json_files:
        minio_client.remove_object(MINIO_BUCKET, json_file)

def check_new_rows():
    session = get_db_session()
    latest_record = session.query(Station).order_by(Station.record_timestamp.desc()).first()
    session.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,  # Set retries to 0 to fail immediately
    'retry_delay': timedelta(seconds=420)  # Set retry delay to 20 seconds
}

with DAG(
    'json2sql_dag2',
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

    process_and_populate_data_task = PythonOperator(
        task_id='process_and_populate_data_task',
        python_callable=process_and_populate_data,
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
    fetch_json_files_task >> process_and_populate_data_task >> check_new_rows_task >> delete_processed_files_task