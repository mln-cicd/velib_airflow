# populate_locations.py

import logging
from app.db.database import get_db_session
from app.db.models import Location, Station
from app.fetch.requests import fetch_data_from_api

logger = logging.getLogger(__name__)

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
            
            
def populate_stations(data):
    logger.info("Populating stations...")
    with get_db_session() as session:
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
    logger.info("Stations populated successfully.")
