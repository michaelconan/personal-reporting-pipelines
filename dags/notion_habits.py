"""
notion_habits.py

DAG to load daily and weekly habits from Notion API into BigQuery.
"""

# Basic imports
import datetime
import pendulum

# Standard airflow imports
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException

# Airflow hooks and operators
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)


@dag(
    schedule="@daily",
    catchup=True,
    start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
    dagrun_timeout=datetime.timedelta(minutes=20),
    tags=["habits", "notion", "staging"],
    # Provide tables via params to enable unit testing
    params={"daily_habit_table": Param("notion_habits_daily")},
)
def NotionHabits():

    # Notion package and details
    from notion_client import Client

    NOTION_CONN_ID = "notion_productivity"
    DAILY_HABIT_DB = "10140b50-0f0d-43d2-905a-7ed714ef7f2c"
    WEEKLY_HABIT_DB = "11e09eb8-3f76-80e7-8fac-e8d0bb538fb0"

    # BigQuery connection details
    BQ_CONN_ID = "bigquery_reporting"
    BQ_DATASET = "staging"
    BQ_LOCATION = "us-central1"

    # Create staging dataset if it doesn't exist
    create_staging_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_staging_dataset",
        gcp_conn_id=BQ_CONN_ID,
        dataset_id=BQ_DATASET,
        location=BQ_LOCATION,
        if_exists="ignore",
    )

    # Create daily habits staging table if it doesn't exist
    create_daily_table = BigQueryCreateEmptyTableOperator(
        task_id="create_staging_notion_habits_daily_table",
        gcp_conn_id=BQ_CONN_ID,
        dataset_id=BQ_DATASET,
        table_id="{{ params.daily_habit_table }}",
        schema_fields=[
            {
                "name": "bq_id",
                "type": "STRING",
                "mode": "REQUIRED",
                "defaultValueExpression": "GENERATE_UUID()",
            },
            {"name": "page_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "database_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "habit", "type": "STRING", "mode": "REQUIRED"},
            {"name": "is_complete", "type": "BOOLEAN", "mode": "REQUIRED"},
            {"name": "page_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "page_edited", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {
                "name": "bq_ts",
                "type": "TIMESTAMP",
                "mode": "REQUIRED",
                "defaultValueExpression": "CURRENT_TIMESTAMP()",
            },
        ],
        if_exists="ignore",
    )

    @task(
        task_id="load_notion_habits_daily_tasks",
    )
    def get_daily_tasks(params: dict):
        # Get task context for data interval
        context = get_current_context()
        dag = context["dag"]
        # Connect to Notion API
        connection = BaseHook.get_connection(NOTION_CONN_ID)
        client = Client(auth=connection.password)

        notion_filters = {
            # Added formula property in database for last edited date to allow filter
            # Use data interval to get records edited within interval
            "and": [
                {
                    "property": "Last Edited",
                    "date": {"after": context["data_interval_start"].isoformat()},
                },
                {
                    "property": "Last Edited",
                    "date": {"before": context["data_interval_end"].isoformat()},
                },
            ]
        }
        # Query the daily habits database
        query_rs = client.databases.query(
            database_id=DAILY_HABIT_DB,
            filter=notion_filters,
        )

        # Pivot the results to a row per habit
        results = [
            (
                r["id"],  # Notion page ID
                DAILY_HABIT_DB,
                r["properties"]["Date"]["date"]["start"][:10],  # Notion task date
                p,  # Notion habit name
                r["created_time"],  # Notion page creation time
                r["last_edited_time"],  # Notion page last edited time
                v["checkbox"],  # Habit completion status
            )
            for r in query_rs["results"]
            # Include only checkbox properties
            for p, v in r["properties"].items()
            if v["type"] == "checkbox"
        ]
        dag.log.info(
            f"{len(results)} daily habits updated for {context['execution_date']}"
        )

        if not results:
            raise AirflowSkipException("No daily habits found for the interval.")
        else:
            # Structure row data for insert query
            data_rows = ", ".join(
                f"({', '.join([repr(v) for v in row])})" for row in results
            )
            insert_rows_query = f"""
                INSERT INTO {BQ_DATASET}.{params["daily_habit_table"]}
                    (page_id, database_id, date, habit, page_created, page_edited, is_complete)
                VALUES {data_rows};
            """

            # Insert daily habits into BigQuery
            # TODO: Break into separate task - how to pass data?
            insert_daily_job = BigQueryInsertJobOperator(
                gcp_conn_id=BQ_CONN_ID,
                task_id="insert_query_job",
                configuration={
                    "query": {
                        "query": insert_rows_query,
                        "useLegacySql": False,
                        "priority": "BATCH",
                    }
                },
                location=BQ_LOCATION,
            )
            insert_daily_job.execute(context=context)

    # Define task flow - task functions must be called ()
    create_staging_dataset >> create_daily_table >> get_daily_tasks()


# Assign the dag to global for execution
dag = NotionHabits()
