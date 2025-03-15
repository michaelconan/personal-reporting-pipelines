# Base imports
import os
import pendulum
import datetime
import shutil

# PyPI imports
import yaml

# Airflow imports
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

# Local imports
from michael.datasets import RAW_DATASETS

@dag(
    dag_id="dbt__michael",
    # Run after source datasets refreshed
    schedule=RAW_DATASETS,
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 1),
    dagrun_timeout=datetime.timedelta(minutes=20),
    params={"dataset": "reporting"},
    tags=["dbt", "transform"],
)
def run_dbt():

    # File paths for service account key and dbt profile
    PROFILES_DIR = "/tmp/.dbt"
    PROJECT_DIR = os.path.join(
        os.path.abspath(os.getenv("AIRFLOW_HOME")), "dbt/michael"
    )
    KEYFILE_PATH = os.path.join(PROFILES_DIR, "bq-service-account.json")
    PROFILE_PATH = os.path.join(PROFILES_DIR, "profiles.yml")

    @task(
        task_id="generate_dbt_profile",
    )
    def generate_dbt_profile(params: dict):
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
                        "dataset": params["dataset"],
                        "project": conn.extra_dejson.get("project"),
                        "location": conn.extra_dejson.get("location"),
                        "priority": "interactive",
                        "job_execution_timeout_seconds": 300,
                        "job_retries": 1,
                        "threads": 1,
                    },
                },
                "target": "dev",
            },
        }
        # Create profile file for dbt run
        with open(PROFILE_PATH, "w") as f:
            yaml.dump(profile, f)

    @task.bash
    def dbt_cli():
        dbt_args = f"--profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR}"
        return f"dbt run {dbt_args}"

    @task(
        task_id="cleanup_files",
        trigger_rule="all_done",
    )
    def cleanup_files():
        # Remove temporary files
        shutil.rmtree(PROFILES_DIR)

    # Define DAG workflow
    generate_dbt_profile() >> dbt_cli() >> cleanup_files()


# Call dag in the global namespace
dbt_dag = run_dbt()
