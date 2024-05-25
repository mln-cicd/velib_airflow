from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests
import logging
import hashlib
import json
import os

# Database connection details
DATABASE_URI = 'postgresql+psycopg2://airflow:airflow@postgres/velib'

# Define the database models and functions directly in the DAG file
def create_tables():
    engine = create_engine(DATABASE_URI)
    with engine.connect() as connection:
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS locations (
            stationcode VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            nom_arrondissement_communes VARCHAR(255)
        );
        """))
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS stations (
            record_timestamp VARCHAR(255) PRIMARY KEY,
            stationcode VARCHAR(255) REFERENCES locations(stationcode),
            ebike INTEGER,
            mechanical INTEGER,
            duedate VARCHAR(255),
            numbikesavailable INTEGER,
            numdocksavailable INTEGER,
            capacity INTEGER,
            is_renting VARCHAR(255),
            is_installed VARCHAR(255),
            is_returning VARCHAR(255)
        );
        """))

def get_db_session():
    engine = create_engine(DATABASE_URI)
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

class Location:
    def __init__(self, stationcode, name, latitude, longitude, nom_arrondissement_communes):
        self.stationcode = stationcode
        self.name = name
        self.latitude = latitude
        self.longitude = longitude
        self.nom_arrondissement_communes = nom_arrondissement_communes

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
    logger.info("Locations populated successfully.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def connect_to_db():
    with get_db_session() as session:
        session.execute(text('SELECT 1'))

def create_tables_if_not_exist():
    create_tables()

def check_locations_empty():
    with get_db_session() as session:
        location_count = session.query(Location).count()
        return location_count == 0

def fetch_data_task():
    return fetch_data_from_api()

def process_data_task():
    data = fetch_data_from_api()
    return process_data(data)

def populate_locations_task(ti):
    data = ti.xcom_pull(task_ids='process_data_task')
    populate_locations(data)

with DAG('stations_init_dag', default_args=default_args, schedule_interval=None) as dag:
    
    connect_to_db_task = PythonOperator(
        task_id='connect_to_db',
        python_callable=connect_to_db
    )

    create_tables_task = PythonOperator(
        task_id='create_tables_if_not_exist',
        python_callable=create_tables_if_not_exist
    )

    check_locations_empty_task = PythonOperator(
        task_id='check_locations_empty',
        python_callable=check_locations_empty
    )

    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_task
    )

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data_task
    )

    populate_locations_task = PythonOperator(
        task_id='populate_locations_task',
        python_callable=populate_locations_task,
        provide_context=True
    )

    connect_to_db_task >> create_tables_task >> check_locations_empty_task
    check_locations_empty_task >> fetch_data_task >> process_data_task >> populate_locations_task