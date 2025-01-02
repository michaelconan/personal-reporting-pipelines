#!/bin/bash

# Run metadata database migrations
airflow db migrate

# Run scheduler and webserver on the same container
airflow scheduler & airflow webserver --port ${PORT:-8080}