"""
notion_habits.py

DAG to merge habit updates into cleansed layer BigQuery table.
"""

# Basic imports
import datetime
import pendulum

# Standard airflow imports
from airflow.decorators import dag, task
from airflow.models import DAG
from airflow.models.param import Param
from airflow.datasets import Dataset

# Airflow hooks and operators
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)

# BigQuery connection details
BQ_CONN_ID = "bigquery_reporting"
BQ_DAILY_TABLE = "notion_daily_habit"
BQ_WEEKLY_TABLE = "notion_weekly_habit"
BQ_HABIT_TABLE = "notion_habit"

# Table schema and SQL query files
HABIT_SCHEMA_FILE = "schema/notion_habit.sql"
MERGE_HABIT_FILE = "sql/merge_notion_habits.sql"

# Datasets for triggering
daily_habits_dataset = Dataset("notion_daily_habits")
weekly_habits_dataset = Dataset("notion_weekly_habits")


@dag(
    schedule=[daily_habits_dataset, weekly_habits_dataset],
    dagrun_timeout=datetime.timedelta(minutes=20),
    start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
    tags=["habits", "notion", "cleansed"],
    user_defined_macros={
        "HABIT_SCHEMA_FILE": HABIT_SCHEMA_FILE,
        "MERGE_HABIT_FILE": MERGE_HABIT_FILE,
        "BQ_DAILY_TABLE": BQ_DAILY_TABLE,
        "BQ_WEEKLY_TABLE": BQ_WEEKLY_TABLE,
        "BQ_HABIT_TABLE": BQ_HABIT_TABLE,
    },
)
def NotionAllHabits(cleansed_dataset: Param = Param("staging")):

    # Create cleansed dataset if it doesn't exist
    create_cleansed_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_cleansed_dataset",
        gcp_conn_id=BQ_CONN_ID,
        dataset_id="{{ params.cleansed_dataset }}",
        if_exists="ignore",
    )

    # Create cleansed table using schema file
    create_habit_table = BigQueryInsertJobOperator(
        task_id="create_cleansed_notion_habit_table",
        gcp_conn_id=BQ_CONN_ID,
        configuration={
            "query": {
                "query": "{% include HABIT_SCHEMA_FILE %}",
                "useLegacySql": False,
                "defaultDataset": {
                    "datasetId": "{{ params.cleansed_dataset }}",
                },
            },
        },
    )

    # Create cleansed table using schema file
    merge_habit_updates = BigQueryInsertJobOperator(
        task_id="merge_notion_habit_updates",
        gcp_conn_id=BQ_CONN_ID,
        configuration={
            "query": {
                "query": "{% include MERGE_HABIT_FILE %}",
                "useLegacySql": False,
                "defaultDataset": {
                    "datasetId": "{{ params.cleansed_dataset }}",
                },
            },
        },
    )

    # Define task flow
    create_cleansed_dataset >> create_habit_table >> merge_habit_updates


# Assign to Globals
habit_dag = NotionAllHabits()
