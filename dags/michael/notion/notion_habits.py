"""
notion_habit_periods.py

DAGs to load daily and weekly habits from Notion API into BigQuery.
"""

# Basic imports
import os
import pendulum
import jsonlines
from typing import Any

# Standard airflow imports
from airflow.decorators import task
from airflow.models import DAG
from airflow.datasets import Dataset

# Airflow hooks and operators
from airflow.hooks.base import BaseHook

# Common custom tasks
from michael.common.bigquery import load_file_to_bq
from michael.datasets import NOTION_DAILY_HABITS_DS, NOTION_WEEKLY_HABITS_DS

IS_TEST = os.getenv("TEST") or os.getenv("CI")

# Notion connection
NOTION_CONN_ID = "notion_productivity"

# BigQuery connection details
BQ_CONN_ID = "bigquery_reporting"
BQ_DAILY_TABLE = "daily_habit"
BQ_WEEKLY_TABLE = "weekly_habit"

# Notion database names
DAILY_DATABASE = "Daily Disciplines"
WEEKLY_DATABASE = "Weekly Disciplines"

# Properties to extract from Notion databases
DAILY_PROPERTIES = [
    "Name",
    "Date",
    "Devotional",
    "Journal",
    "Prayer",
    "Read Bible",
    "Workout",
    "Language",
]
WEEKLY_PROPERTIES = [
    "Name",
    "Date",
    "Church",
    "Fast",
    "Community",
    "Prayer Minutes",
    "Screen Minutes",
]

# Configurations to generated DAGs
DAG_CONFIGS = [
    {
        "dag_id": "load_initial_daily_habits",
        "schedule": "@once",
        "bq_table": BQ_DAILY_TABLE,
        "dataset": NOTION_DAILY_HABITS_DS,
        "database": DAILY_DATABASE,
        "properties": DAILY_PROPERTIES,
    },
    {
        "dag_id": "load_changed_daily_habits",
        "schedule": "@daily",
        "bq_table": BQ_DAILY_TABLE,
        "dataset": NOTION_DAILY_HABITS_DS,
        "database": DAILY_DATABASE,
        "properties": DAILY_PROPERTIES,
    },
    {
        "dag_id": "load_initial_weekly_habits",
        "schedule": "@once",
        "bq_table": BQ_WEEKLY_TABLE,
        "dataset": NOTION_WEEKLY_HABITS_DS,
        "database": WEEKLY_DATABASE,
        "properties": WEEKLY_PROPERTIES,
    },
    {
        "dag_id": "load_changed_weekly_habits",
        "schedule": "@daily",
        "bq_table": BQ_WEEKLY_TABLE,
        "dataset": NOTION_WEEKLY_HABITS_DS,
        "database": WEEKLY_DATABASE,
        "properties": WEEKLY_PROPERTIES,
    },
]


def create_notion_dag(
    dag_id: str,
    schedule: str,
    bq_table: str,
    dataset: Dataset,
    database: str,
    properties: list[str],
) -> DAG:
    with DAG(
        dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2024, 10, 1),
        catchup=False,
        params={"raw_dataset": "raw"},
        user_defined_macros={"BQ_TABLE": bq_table},
        tags=["notion", "habits", "raw"],
    ) as dag:
        DATA_FILE = "/tmp/notion_data.jsonl"

        @task(
            task_id="get_notion_habit_pages",
        )
        def get_notion_habits(
            conn_id: str,
            database_name: str,
            properties: list[str],
            file_path: str,
            # Context variables
            dag: DAG,
            ds: str,
            data_interval_start: pendulum.DateTime,
            data_interval_end: pendulum.DateTime,
        ) -> int:
            """Shared task to export Notion habit page data to a file

            Args:
                conn_id (str): Airflow connection identifier for Notion API key
                database_name (str): Notion database name
                properties (list[str]): List of properties to extract from habit pages
                file_path (str): JSON file path to export data
                dag (DAG): DAG calling task, from context
                ds (str): ISO logical date of DAG, from context
                data_interval_start (pendulum.DateTime): Start of interval, from context
                data_interval_end (pendulum.DateTime): End of interval, from context

            Raises:
                AirflowSkipException: No data identified for interval
            """

            def get_edited_notion_pages(
                conn_id: str,
                database_name: str,
                start_date: pendulum.DateTime,
                end_date: pendulum.DateTime,
            ) -> list[dict]:
                """Query Notion Database via API for edited pages

                Args:
                    conn_id (str): Airflow connection identifier for Notion API key
                    database_name (str): Name of Notion database
                    start_date (pendulum.DateTime): Minimum timestamp for edited pages
                    end_date (pendulum.DateTime): Maximum timestamp for edited pages

                Returns:
                    list[dict]: Pages edited within specified interval
                """
                from notion_client import Client

                # Connect to Notion API
                connection = BaseHook.get_connection(conn_id)
                client = Client(auth=connection.password)

                # Identify database by name
                search_results = client.search(
                    query=database_name,
                    filter={"property": "object", "value": "database"},
                )
                database_id = None
                if search_results:
                    matched_results = [
                        r
                        for r in search_results["results"]
                        if database_name in "".join(t["plain_text"] for t in r["title"])
                    ]
                    if matched_results:
                        database_id = matched_results[0]["id"]

                if not database_id:
                    raise LookupError(f"Database '{database_name}' not found.")

                # Create request body for database query
                body = {"database_id": database_id}
                # @once DAGs have no interval, so query all records for initial load
                if start_date != end_date:
                    body["filters"] = {
                        # Added formula property in database for last edited time to allow filter
                        # Use data interval to get records edited within interval
                        "and": [
                            {
                                "property": "Last edited time",
                                "date": {"after": start_date.isoformat()},
                            },
                            {
                                "property": "Last edited time",
                                "date": {"before": end_date.isoformat()},
                            },
                        ]
                    }

                all_results = list()
                cursor = None
                # Query the daily habits database, paginate to capture all results
                while True:
                    if cursor:
                        body["start_cursor"] = cursor
                    query_results = client.databases.query(**body)
                    all_results.extend(query_results["results"])
                    # Only load first page of results for testing
                    if not query_results["has_more"] or IS_TEST:
                        break
                    cursor = query_results["next_cursor"]

                return database_id, all_results

            def get_notion_property(property_dict: dict[str, Any]) -> Any:
                """Extract property value from Notion page data"""
                property_type = property_dict["type"]
                if property_type == "title":
                    return property_dict["title"][0]["plain_text"]
                elif property_type == "checkbox":
                    return property_dict["checkbox"]
                elif property_type == "date":
                    return property_dict["date"]["start"][:10]
                elif property_type == "number":
                    return property_dict["number"]
                elif property_type == "select":
                    return property_dict["select"]["name"]
                elif property_type == "formula":
                    return get_notion_property(property_dict["formula"])

            def get_page_data(
                database_id: str, page: dict, properties: list[str]
            ) -> dict:
                property_data = {
                    prop: get_notion_property(page["properties"][prop])
                    for prop in properties
                }
                return {
                    "database_id": database_id,
                    "id": page["id"],
                    **property_data,
                    "created_time": page["created_time"],
                    "last_edited_time": page["last_edited_time"],
                }

            # Get pages edited within the interval from database
            database_id, edited_pages = get_edited_notion_pages(
                conn_id=conn_id,
                database_name=database_name,
                start_date=data_interval_start,
                end_date=data_interval_end,
            )

            results = [
                get_page_data(database_id=database_id, page=page, properties=properties)
                for page in edited_pages
            ]
            dag.log.info(f"{len(results)} habits updated for {ds}")

            # Check if pages were returned for data interval
            if results:
                with jsonlines.open(file_path, mode="w") as writer:
                    writer.write_all(results)

            return len(results)

        @task(
            task_id="load_file_to_bq",
            outlets=[dataset],
        )
        def load_data_file(rows: int, params: dict, outlet_events=None):
            if rows > 0:
                job_state = load_file_to_bq(
                    conn_id=BQ_CONN_ID,
                    file_path=DATA_FILE,
                    table_id=f"{params['raw_dataset']}.{bq_table}",
                )
                # Update outlet dataset extras
                outlet_events[daily_habits_dataset].extra = {
                    "state": job_state,
                    "rows": rows,
                }

        # Define task flow - task functions must be called ()
        notion_habits = get_notion_habits(
            conn_id=NOTION_CONN_ID,
            database_name=database,
            properties=properties,
            file_path=DATA_FILE,
        )
        load_data_file(notion_habits)

        return dag


# Create DAGs dynamically
for config in DAG_CONFIGS:
    globals()[config["dag_id"]] = create_notion_dag(**config)
