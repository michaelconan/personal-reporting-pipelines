"""Pipeline configuration module.

This module contains global configuration settings for all data pipelines,
including schema definitions, environment variables, and base constants.
"""

import os

# IS_TEST = os.getenv("TEST") or os.getenv("CI")
BASE_DATE = "2020-01-01"

# google or 1password
SECRET_STORE = os.getenv("SECRET_STORE", default="1password")

# Load and set dynamic schema and table variables based on DBT target
DBT_TARGET = os.getenv("DBT_TARGET", default="prod")
suffix = f"_{DBT_TARGET}" if DBT_TARGET != "prod" else ""

# Define raw and DBT schemas
DBT_SCHEMA_NAME = os.getenv("DBT_SCHEMA_NAME", default="reporting")
DBT_SCHEMA = DBT_SCHEMA_NAME + suffix
RAW_SCHEMA = os.getenv("RAW_SCHEMA_NAME", default="raw") + suffix

# Set environment variables for schemas
os.environ["DBT_SCHEMA"] = DBT_SCHEMA
os.environ["RAW_SCHEMA"] = RAW_SCHEMA
