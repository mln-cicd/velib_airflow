# fetch_api2sql.py

from prefect import flow, task
from plugins.fetch.requests import fetch_data_from_api, check_same_md5_hash, process_data
from plugins.fetch.populate_databases import populate_locations, populate_stations
from plugins.db.models import (
    Location, 
    Station
)


@task
def fetch_data_task():
    fetch_data_from_api()


@task
def check_md5_hash_task():
    is_same_hash = check_same_md5_hash()
    if is_same_hash:
        raise ValueError("MD5 hash is the same as the previous API call. Skipping further processing.")


@task
def process_data_task():
    return process_data()


@task
def populate_locations_task(processed_data):
    populate_locations(processed_data)


@task
def populate_stations_task(processed_data):
    populate_stations(processed_data)


@flow(name="fetch_api2sql")
def fetch_api2sql_flow():
    fetch_data_task()
    check_md5_hash_task()
    processed_data = process_data_task()
    populate_locations_task(processed_data)
    populate_stations_task(processed_data)

    return fetch_api2sql_flow

# if __name__ == '__main__':
#     fetch_api2sql_flow()