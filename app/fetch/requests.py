# api.py
import os
import hashlib
import requests
import json
import logging

from app import MD5_FILE, DL_DIR, DL_FILE

logger = logging.getLogger(__name__)

def fetch_data_from_api():
    logger.info("Fetching data from API...")
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&timezone=Europe/Paris&rows=2000"
    response = requests.get(url, headers={"accept": "application/json"})
    data = response.json()

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

def check_same_md5_hash():
    logger.info("Checking MD5 hash...")
    with open('records/check.md5', 'r') as file:
        current_md5 = file.read().strip()

    try:
        with open('records/previous.md5', 'r') as file:
            previous_md5 = file.read().strip()
        logger.info("Comparing MD5 hashes: Current - %s, Previous - %s", current_md5, previous_md5)
        return previous_md5 == current_md5
    except FileNotFoundError:
        logger.warning("Previous MD5 hash file not found.")
        return False

def process_data(file_path=DL_FILE):
    logger.info("Processing data from file: %s", file_path)
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        logger.error("File not found: %s", file_path)
        return []
    except json.JSONDecodeError:
        logger.error("Invalid JSON format in file: %s", file_path)
        return []
    
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

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    fetch_data_from_api()
