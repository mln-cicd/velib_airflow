from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text, Column, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
import hashlib
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

# Define the Location ORM model
class Location(Base):
    __tablename__ = 'locations'
    stationcode = Column(String, primary_key=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    nom_arrondissement_communes = Column(String)

# Define the database models and functions directly in the DAG file
def create_tables():
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)

def get_db_session():
    engine = create_engine(DATABASE_URI)
    Session = sessionmaker(bind=engine)
    return Session()

logger = logging.getLogger(__name__)

def check_locations_empty():
    with get_db_session() as session:
        location_count = session.query(Location).count()
        return location_count == 0

def fetch_data_from_minio():
    logger.info("Fetching data from MinIO bucket...")
    try:
        objects = minio_client.list_objects(MINIO_BUCKET, recursive=True)
        json_files = [obj.object_name for obj in objects if obj.object_name.endswith('.json')]
        
        if not json_files:
            logger.warning("No JSON files found in MinIO bucket.")
            return None
        
        latest_file = max(json_files, key=lambda x: x.split('_')[1] + x.split('_')[2].split('.')[0])
        logger.info("Latest JSON file: %s", latest_file)
        
        json_obj = minio_client.get_object(MINIO_BUCKET, latest_file)
        data = json.load(json_obj)
        return data
    except S3Error as e:
        logger.error("Failed to get JSON data from MinIO: %s", e)
        raise

def process_data(data):
    logger.info("Processing data...")
    records = data.get("records", [])
    processed_data = []
    for record in records:
        fields = record.get("fields", {})
        processed_data.append({
            "name": fields.get("name", ""),
            "stationcode": fields.get("stationcode", ""),
            "latitude": fields.get("coordonnees_geo", [None, None])[0],
            "longitude": fields.get("coordonnees_geo", [None, None])[1],
            "nom_arrondissement_communes": fields.get("nom_arrondissement_communes", "")
        })
    logger.info("Data processing completed. Processed %d records.", len(processed_data))
    return processed_data

def populate_locations(data):
    logger.info("Populating locations...")
    with get_db_session() as session:
        for record in data:
            logger.debug("Processing record: %s", record)
            location = Location(
                stationcode=record['stationcode'],
                name=record['name'],
                latitude=record['latitude'],
                longitude=record['longitude'],
                nom_arrondissement_communes=record['nom_arrondissement_communes'],
            )
            session.merge(location)
            logger.debug("Merged location: %s", location)
        session.commit()  # Ensure the session is committed
    logger.info("Locations populated successfully.")

def check_station_codes_count():
    with get_db_session() as session:
        location_count = session.query(Location).count()
        if location_count > 1400:
            logger.info("There are more than 1400 station codes in the locations table.")
            return True
        else:
            logger.info("There are not enough station codes in the locations table.")
            return False

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('locations_init_dag', default_args=default_args, schedule_interval=None) as dag:
    
    connect_to_db_task = PythonOperator(
        task_id='connect_to_db',
        python_callable=lambda: create_tables()
    )

    check_locations_empty_task = PythonOperator(
        task_id='check_locations_empty',
        python_callable=check_locations_empty
    )

    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_minio
    )

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=lambda ti: process_data(ti.xcom_pull(task_ids='fetch_data_task'))
    )

    populate_locations_task = PythonOperator(
        task_id='populate_locations_task',
        python_callable=lambda ti: populate_locations(ti.xcom_pull(task_ids='process_data_task'))
    )

    check_station_codes_count_task = PythonOperator(
        task_id='check_station_codes_count',
        python_callable=check_station_codes_count
    )

    connect_to_db_task >> check_locations_empty_task
    check_locations_empty_task >> fetch_data_task >> process_data_task >> populate_locations_task >> check_station_codes_count_task
    check_locations_empty_task >> check_station_codes_count_task