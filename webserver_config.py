"""
webserver_config.py

Configuration of Airflow webserver and definition of Flask app object,
which may be beneficial for Azure app service custom startup.
"""
from flask import current_app as app
