CREATE SCHEMA IF NOT EXISTS dev;
CREATE TABLE IF NOT EXISTS dev.raw_earthquake_records(
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT NOW(),
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    depth_km FLOAT,
    magnitude FLOAT NOT NULL,
    location TEXT,
    CONSTRAINT unique_earthquake UNIQUE(datetime, latitude, longitude, magnitude));

CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow_db OWNER airflow;