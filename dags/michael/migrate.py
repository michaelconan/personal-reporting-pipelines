# Base imports
import os
from datetime import datetime

# Airflow imports
from airflow.models import DAG
from airflow.decorators import task

# Airflow hooks and operators
from airflow.hooks.base import BaseHook
from airflow_provider_alembic.operators.alembic import AlembicOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)

# Get migration folder relative to DAG
migration_folder = os.path.join(os.path.dirname(__file__), "migrations")

DATASET = os.getenv("ADMIN_DATASET", "admin")

with DAG(
    "bq__migrate_schema",
    schedule="@once",  # also consider "None"
    start_date=datetime(1970, 1, 1),
    params={"command": "upgrade", "revision": "head"},
    tags=["raw", "migrate"],
) as dag:

    KEYFILE_PATH = "/tmp/bq-service-account.json"
    BIGQUERY_CONN_ID = "bigquery_reporting"
    # Set keyfile as application default credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEYFILE_PATH

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_admin_dataset",
        gcp_conn_id=BIGQUERY_CONN_ID,
        dataset_id=DATASET,
        if_exists="ignore",
    )

    @task(task_id="save_bigquery_keyfile")
    def get_keyfile():
        # Get BigQuery connection details
        conn = BaseHook.get_connection(BIGQUERY_CONN_ID)

        # Write keyfile to temporary file
        with open(KEYFILE_PATH, "w") as f:
            f.write(conn.extra_dejson.get("keyfile_dict"))

    # Run migrations
    alembic_op = AlembicOperator(
        task_id="run_alembic_migrations",
        conn_id=BIGQUERY_CONN_ID,
        command="{{ params.command }}",
        revision="{{ params.revision }}",
        script_location=migration_folder,
    )

    @task(task_id="cleanup_keyfile", trigger_rule="all_done")
    def cleanup_keyfile():
        # Remove keyfile
        if os.path.exists(KEYFILE_PATH):
            os.remove(KEYFILE_PATH)
        else:
            dag.log.info(f"Keyfile not found at: {KEYFILE_PATH}")

    create_dataset >> get_keyfile() >> alembic_op >> cleanup_keyfile()
