import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))



from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.fetch.requests import fetch_data_from_api, check_same_md5_hash, process_data
from plugins.fetch.populate_databases import populate_stations

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def fetch_data_task():
    fetch_data_from_api()

def check_md5_hash_task():
    is_same_hash = check_same_md5_hash()
    if is_same_hash:
        raise ValueError("MD5 hash is the same as the previous API call. Skipping further processing.")

def process_data_task():
    return process_data()

def populate_stations_task(ti):
    processed_data = ti.xcom_pull(task_ids='process_data_task')
    populate_stations(processed_data)

with DAG(
    'fetch_api2sql_dag',
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