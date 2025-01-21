# Base imports
import os
import pendulum
import datetime

# PyPI imports
import yaml

# Airflow imports
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

# Dataset name in BigQuery for DBT
DBT_DATASET = "reporting"


@dag(
    schedule="@daily",
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 1),
    dagrun_timeout=datetime.timedelta(minutes=20),
)
def DBT():

    # File paths for service account key and dbt profile
    PROFILES_DIR = "/tmp/.dbt"
    KEYFILE_PATH = os.path.join(PROFILES_DIR, "bq-service-account.json")
    PROFILE_PATH = os.path.join(PROFILES_DIR, "dbt_profile.yml")

    @task(
        task_id="generate_dbt_profile",
    )
    def generate_dbt_profile():
        # Get BigQuery connection details
        conn = BaseHook.get_connection("bigquery_reporting")

        # Write keyfile to temporary file
        os.makedirs(os.path.dirname(KEYFILE_PATH), exist_ok=True)
        with open(KEYFILE_PATH, "w") as f:
            f.write(conn.extra_dejson.get("keyfile_dict"))

        # Generate profile with BigQuery details
        profile = {
            "michael": {
                "outputs": {
                    "dev": {
                        "type": "bigquery",
                        "method": "service-account",
                        "keyfile": KEYFILE_PATH,
                        "dataset": DBT_DATASET,
                        "project": conn.extra_dejson.get("project"),
                        "location": conn.extra_dejson.get("location"),
                        "priority": "interactive",
                        "job_execution_timeout_seconds": 300,
                        "job_retries": 1,
                        "threads": 1,
                    },
                },
                "target": "dev",
            }
        }
        # Create profile file for dbt run
        with open(PROFILE_PATH, "w") as f:
            yaml.dump(profile, f)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {PROFILES_DIR}",
        env={"DBT_PROFILES_DIR": PROFILES_DIR},
    )

    @task(
        task_id="cleanup_files",
    )
    def cleanup_files():
        # Remove temporary files
        os.remove(PROFILES_DIR)

    # Define DAG workflow
    generate_dbt_profile() >> dbt_run >> cleanup_files()
