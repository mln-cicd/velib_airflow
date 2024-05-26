#!/bin/bash

# Create the mount/logs directory if it doesn't exist
mkdir -p ./mount/logs

# Copy worker logs recursively from the container to the mount/logs directory
docker cp velib_airflow-airflow-worker-1:/opt/airflow/logs/. ./mount/logs/
