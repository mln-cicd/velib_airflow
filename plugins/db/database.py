# database.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
from app import DB_URL
import logging

Base = declarative_base()
logger = logging.getLogger(__name__)

def create_database():
    logger.info("Creating database connection...")
    logger.info("DB_URL: %s", DB_URL)
    engine = create_engine(DB_URL, pool_size=10, max_overflow=20)
    logger.info("Database connection created successfully.")
    return engine

@contextmanager
def get_db_session():
    logger.info("Getting database session...")
    engine = create_database()
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        logger.info("Yielding database session...")
        yield session
        logger.info("Committing database session...")
        session.commit()
    except Exception as e:
        logger.error("An error occurred. Rolling back database session...")
        logger.exception(e)
        session.rollback()
        raise
    finally:
        logger.info("Closing database session...")
        session.close()

engine = create_database()
Session = sessionmaker(bind=engine)

def create_tables():
    logger.info("Creating database tables...")
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully.")
