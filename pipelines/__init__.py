"""Pipeline configuration module.

This module contains global configuration settings for all data pipelines,
including schema definitions, environment variables, and base constants.
"""

import os

# IS_TEST = os.getenv("TEST") or os.getenv("CI")
BASE_DATE = "2020-01-01"

# google or 1password
SECRET_STORE = os.getenv("SECRET_STORE", default="1password")

# Define schema names supplied by the environment, defaulting to local dev
DBT_SCHEMA = os.getenv("DBT_SCHEMA") or "reporting_dev"
RAW_SCHEMA = os.getenv("RAW_SCHEMA") or "raw_dev"

# Set environment variables for schemas
os.environ["DBT_SCHEMA"] = DBT_SCHEMA
os.environ["RAW_SCHEMA"] = RAW_SCHEMA
