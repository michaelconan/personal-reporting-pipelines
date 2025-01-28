# Base imports
import os
from datetime import datetime

# Airflow imports
from airflow.models import DAG
from airflow.decorators import task
from airflow.models.param import Param

# Airflow hooks and operators
from airflow.hooks.base import BaseHook
from airflow_provider_alembic.operators.alembic import AlembicOperator

# Get migration folder relative to DAG
migration_folder = os.path.join(os.path.dirname(__file__), "migrations")

with DAG(
    "migrate_raw_tables",
    schedule="@once",  # also consider "None"
    start_date=datetime(1970, 1, 1),
    params={"command": Param("upgrade"), "revision": Param("head")},
    tags=["raw", "migrate"],
) as dag:

    KEYFILE_PATH = "/tmp/bq-service-account.json"
    BIGQUERY_CONN_ID = "bigquery_reporting"
    # Set keyfile as application default credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEYFILE_PATH

    @task(task_id="bigquery_keyfile")
    def get_keyfile():
        # Get BigQuery connection details
        conn = BaseHook.get_connection(BIGQUERY_CONN_ID)

        # Write keyfile to temporary file
        with open(KEYFILE_PATH, "w") as f:
            f.write(conn.extra_dejson.get("keyfile_dict"))

    # Run migrations
    alembic_op = AlembicOperator(
        task_id="alembic_op",
        conn_id=BIGQUERY_CONN_ID,
        command="{{ params.command }}",
        revision="{{ params.revision }}",
        script_location=migration_folder,
    )

    get_keyfile() >> alembic_op
