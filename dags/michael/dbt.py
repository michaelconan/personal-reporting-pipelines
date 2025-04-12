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
from airflow.configuration import conf

# Local imports
from michael.datasets import RAW_DATASETS

# File paths for service account key and dbt profile
PROFILES_DIR = "/tmp/.dbt"
# Assume dbt project is in the same directory as the DAGs folder
DAGS_FOLDER = conf.get("core", "dags_folder")
PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(DAGS_FOLDER)), "dbt/michael")
KEYFILE_PATH = os.path.join(PROFILES_DIR, "bq-service-account.json")
PROFILE_PATH = os.path.join(PROFILES_DIR, "profiles.yml")

TARGETS = (
    "dev",
    "test",
    "prod",
)


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
    profile_outputs = {
        "type": "bigquery",
        "method": "service-account",
        "keyfile": KEYFILE_PATH,
        "project": conn.extra_dejson.get("project"),
        "location": conn.extra_dejson.get("location"),
        "priority": "interactive",
        "job_execution_timeout_seconds": 300,
        "job_retries": 1,
        "threads": 1,
    }
    profile = {
        "michael": {
            "outputs": {
                t: {
                    **profile_outputs.copy(),
                    **{"dataset": (f"{t}_" if t != "prod" else "") + "reporting"},
                }
                for t in TARGETS
            },
            "target": params["target"],
        },
    }
    # Create profile file for dbt run
    with open(PROFILE_PATH, "w") as f:
        yaml.dump(profile, f)


@task.bash
def dbt_cli(command: str) -> str:
    dbt_args = f"--profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR}"
    return f"dbt {command} {dbt_args}"


@task(
    task_id="cleanup_files",
    trigger_rule="all_done",
)
def cleanup_files():
    # Remove temporary files
    shutil.rmtree(PROFILES_DIR)


@dag(
    dag_id="dbt__michael",
    # Run after source datasets refreshed
    schedule=RAW_DATASETS,
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 1),
    dagrun_timeout=datetime.timedelta(minutes=20),
    params={"target": "prod"},
    tags=["dbt", "transform"],
)
def run_dbt():
    # Define DAG workflow
    (
        generate_dbt_profile()
        >> dbt_cli("deps")
        >> dbt_cli("run")
        >> dbt_cli("test")
        >> cleanup_files()
    )


@dag(
    dag_id="dbt_docs__michael",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 1),
    dagrun_timeout=datetime.timedelta(minutes=20),
    params={"dataset": "reporting"},
    tags=["dbt", "docs"],
)
def make_dbt_docs():
    # Define DAG workflow
    (
        generate_dbt_profile()
        >> dbt_cli("deps")
        >> dbt_cli("docs generate --static")
        >> cleanup_files()
    )


# Call dag in the global namespace
dbt_dag = run_dbt()
dbt_docs = make_dbt_docs()
