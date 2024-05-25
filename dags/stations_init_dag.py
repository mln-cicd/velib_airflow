from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import text
from app import DL_FILE
from app.db.database import create_tables, get_db_session
from app.fetch.requests import fetch_data_from_api, process_data
from app.db.models import Location
from app.fetch.populate_databases import populate_locations

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
    fetch_data_from_api()

def process_data_task():
    return process_data(DL_FILE)

def populate_locations_task(ti):
    data = ti.xcom_pull(task_ids='process_data_task')
    populate_locations(data)

with DAG('startup_application_dag', default_args=default_args, schedule_interval=None) as dag:
    
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