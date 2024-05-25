# orchestration/tasks/app_startup.py
from sqlalchemy import text
from prefect import task, Flow, flow
from app import DL_FILE
from app.db.database import create_tables, get_db_session
from app.fetch.requests import fetch_data_from_api, process_data

from app.db.models import Location
from app.fetch.populate_databases import populate_locations

@task
def start_prefect():
    pass

@task
def connect_to_db():
    with get_db_session() as session:
        session.execute(text('SELECT 1'))
        

@task
def create_tables_if_not_exist():
    create_tables()
    
    
@task
def check_locations_empty():
    with get_db_session() as session:
        location_count = session.query(Location).count()
        return location_count == 0

@task
def fetch_data_task():
    fetch_data_from_api()

@task
def process_data_task():
    return process_data(DL_FILE)

@task
def populate_locations_task(data):
    populate_locations(data)

@flow(log_prints=True)
def startup_application_flow():
    prefect_started = start_prefect()
    db_connected = connect_to_db()
    tables_created = create_tables_if_not_exist()
    
    locations_empty = check_locations_empty()
    
    if locations_empty:
        fetch_data_task()
        processed_data = process_data_task()
        populate_locations_task(processed_data)




# if __name__ == "__main__":
#     startup_application.serve(name="startup")