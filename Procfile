web: airflow webserver --hostname 0.0.0.0 --port ${PORT:-8080} --workers 4
scheduler: airflow db migrate && airflow scheduler