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
    id SERIAL PRIMARY KEY,
    record_timestamp VARCHAR(255),
    stationcode VARCHAR(255),
    ebike INTEGER,
    mechanical INTEGER,
    duedate VARCHAR(255),
    numbikesavailable INTEGER,
    numdocksavailable INTEGER,
    capacity INTEGER,
    is_renting VARCHAR(255),
    is_installed VARCHAR(255),
    is_returning VARCHAR(255),
    FOREIGN KEY (stationcode) REFERENCES locations(stationcode)
);

-- Create the velib_user and grant privileges
DO
$$
BEGIN
    IF NOT EXISTS (
        SELECT
        FROM   pg_catalog.pg_roles
        WHERE  rolname = 'velib_user') THEN

        CREATE ROLE velib_user LOGIN PASSWORD 'velib_password';
    END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE velib TO velib_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO velib_user;
