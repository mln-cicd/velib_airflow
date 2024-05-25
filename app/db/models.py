# models.py
from sqlalchemy import Column, String, Float, ForeignKey, Integer
from app.db.database import Base


class Location(Base):
    __tablename__ = 'locations'
    stationcode = Column(String, primary_key=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    nom_arrondissement_communes = Column(String)




class Station(Base):
    __tablename__ = 'stations'
    record_timestamp = Column(String, primary_key=True)
    stationcode = Column(String, ForeignKey('locations.stationcode'))
    ebike = Column(Integer)
    mechanical = Column(Integer)
    duedate = Column(String)
    numbikesavailable = Column(Integer)
    numdocksavailable = Column(Integer)
    capacity = Column(Integer)
    is_renting = Column(String)
    is_installed = Column(String)
    is_returning = Column(String)