-- Create the velib database if it doesn't exist
SELECT 'CREATE DATABASE velib' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'velib')\gexec

-- Connect to the velib database
\c velib;

-- Create the locations table if it doesn't exist
CREATE TABLE IF NOT EXISTS locations (
    stationcode VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    nom_arrondissement_communes VARCHAR(255)
);

-- Create the stations table if it doesn't exist
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

-- Create the prefect database if it doesn't exist
SELECT 'CREATE DATABASE prefect' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'prefect')\gexec