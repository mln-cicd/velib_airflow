from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
import logging
import hashlib
import json
import os

# Database connection details
DATABASE_URI = 'postgresql+psycopg2://velib_user:velib_password@user-postgres/velib'

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

# Define the Station ORM model
class Station(Base):
    __tablename__ = 'stations'
    record_timestamp = Column(String, primary_key=True)
    stationcode = Column(String)
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
    return data

def check_same_md5_hash():
    logger.info("Checking MD5 hash...")
    with open('/tmp/data.md5', 'r') as file:
        current_md5 = file.read().strip()

    try:
        with open('/tmp/previous.md5', 'r') as file:
            previous_md5 = file.read().strip()
        logger.info("Comparing MD5 hashes: Current - %s, Previous - %s", current_md5, previous_md5)
        return previous_md5 == current_md5
    except FileNotFoundError:
        logger.warning("Previous MD5 hash file not found.")
        return False

def process_data(data):
    logger.info("Processing data...")
    records = data.get("records", [])
    processed_data = []
    for record in records:
        fields = record.get("fields", {})
        processed_data.append({
            "name": fields.get("name", ""),
            "stationcode": fields.get("stationcode", ""),
            "ebike": fields.get("ebike", 0),
            "mechanical": fields.get("mechanical", 0),
            "latitude": fields.get("coordonnees_geo", [None, None])[0],
            "longitude": fields.get("coordonnees_geo", [None, None])[1],
            "duedate": fields.get("duedate", ""),
            "numbikesavailable": fields.get("numbikesavailable", 0),
            "numdocksavailable": fields.get("numdocksavailable", 0),
            "capacity": fields.get("capacity", 0),
            "is_renting": fields.get("is_renting", ""),
            "is_installed": fields.get("is_installed", ""),
            "nom_arrondissement_communes": fields.get("nom_arrondissement_communes", ""),
            "is_returning": fields.get("is_returning", ""),
            "record_timestamp": record.get("record_timestamp", "")
        })
    logger.info("Data processing completed. Processed %d records.", len(processed_data))
    return processed_data

def populate_stations(data):
    logger.info("Populating stations...")
    session = get_db_session()
    for record in data:
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
        session.merge(station)
        logger.debug("Merged station: %s", station)
    session.commit()
    logger.info("Stations populated successfully.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def fetch_data_task():
    return fetch_data_from_api()

def check_md5_hash_task():
    return check_same_md5_hash()

def process_data_task():
    data = fetch_data_from_api()
    return process_data(data)

def populate_stations_task(ti):
    processed_data = ti.xcom_pull(task_ids='process_data_task')
    populate_stations(processed_data)

with DAG(
    'api2sql_dag',
    default_args=default_args,
    schedule_interval=timedelta(seconds=120),
    catchup=False
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_task
    )

    check_md5_hash = PythonOperator(
        task_id='check_md5_hash_task',
        python_callable=check_md5_hash_task
    )

    process_data = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data_task
    )

    populate_stations = PythonOperator(
        task_id='populate_stations_task',
        python_callable=populate_stations_task,
        provide_context=True
    )

    fetch_data >> check_md5_hash >> process_data >> populate_stations