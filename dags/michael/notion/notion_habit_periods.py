"""
notion_habit_periods.py

DAGs to load daily and weekly habits from Notion API into BigQuery.
"""

# Basic imports
import datetime
import pendulum
import jsonlines

# Standard airflow imports
from airflow.decorators import dag, task
from airflow.models import DAG
from airflow.models.param import Param
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException

# Airflow hooks and operators
from airflow.hooks.base import BaseHook

# Common custom tasks
from michael.common.bigquery import load_file_to_bq

# Notion connection
NOTION_CONN_ID = "notion_productivity"

# BigQuery connection details
BQ_CONN_ID = "bigquery_reporting"
BQ_DAILY_TABLE = "notion_daily_habit"
BQ_WEEKLY_TABLE = "notion_weekly_habit"

# Datasets for triggering
daily_habits_dataset = Dataset("notion_daily_habits")
weekly_habits_dataset = Dataset("notion_weekly_habits")


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
            query=database_name, filter={"property": "object", "value": "database"}
        )
        if not search_results:
            raise LookupError(f"Database {database_name} not found.")
        else:
            search_results = [r for r in search_results if database_name in r["title"]]
            database_id = search_results[0]["id"]

        notion_filters = {
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
        # Query the daily habits database
        query_rs = client.databases.query(
            database_id=database_id,
            filter=notion_filters,
        )

        return database_id, query_rs["results"]

    def get_notion_property(property_dict: dict, property_name: str):
        """Extract property value from Notion page data"""
        property_type = property_dict["type"]
        if property_type == "title":
            return property["title"][0]["plain_text"]
        elif property_type == "checkbox":
            return property["checkbox"]
        elif property_type == "date":
            return property["date"]["start"]
        elif property_type == "number":
            return property["number"]
        elif property_type == "select":
            return property["select"]["name"]
        elif property_type == "formula":
            return get_notion_property(property_dict["formula"])

    def get_page_data(database_id: str, page: dict, properties: list[str]) -> dict:
        property_data = {
            prop: get_notion_property(page["properties"], prop) for prop in properties
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


@dag(
    schedule="@daily",
    catchup=True,
    start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
    dagrun_timeout=datetime.timedelta(minutes=20),
    tags=["habits", "notion", "raw"],
    user_defined_macros={
        "BQ_DAILY_TABLE": BQ_DAILY_TABLE,
    },
)
def NotionDailyHabits(raw_dataset: Param = Param("raw")):
    """Extract and load daily habit data from Notion database

    Tasks:
        - Export updated habit pages from Notion to a file
        - Insert habit data from file into BigQuery table

    Args:
        raw_dataset (str, optional): Raw dataset to allow testing override.
            Defaults to "raw".
    """
    DATA_FILE = "/tmp/daily_habits.jsonl"

    @task(
        task_id="load_file_to_bq",
        outlets=[daily_habits_dataset],
    )
    def load_daily_data_file(rows: int, params: dict, outlet_events=None):
        if rows > 0:
            job_state = load_file_to_bq(
                conn_id=BQ_CONN_ID,
                file_path=DATA_FILE,
                table_id=f"{params['raw_dataset']}.{BQ_DAILY_TABLE}",
            )
            # Update outlet dataset extras
            outlet_events[daily_habits_dataset].extra = {
                "state": job_state,
                "rows": rows,
            }

    # Define task flow - task functions must be called ()
    daily_habits = get_notion_habits(
        conn_id=NOTION_CONN_ID,
        database_name="Daily Disciplines",
        properties=[
            "Name",
            "Date",
            "Devotional",
            "Journal",
            "Prayer",
            "Read Bible",
            "Workout",
            "Language",
        ],
        file_path=DATA_FILE,
    )
    load_daily_data_file(daily_habits)


@dag(
    schedule="@weekly",
    catchup=True,
    start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
    dagrun_timeout=datetime.timedelta(minutes=20),
    tags=["habits", "notion", "raw"],
    user_defined_macros={
        "BQ_WEEKLY_TABLE": BQ_WEEKLY_TABLE,
    },
)
def NotionWeeklyHabits(raw_dataset: Param = Param("raw")):
    """Extract and load weekly habit data from Notion database

    Tasks:
        - Export updated habit pages from Notion to a file
        - Insert habit data from file into BigQuery table

    Args:
        raw_dataset (Param, optional): Dataset name to allow testing override.
            Defaults to "raw".
    """
    WEEKLY_HABIT_DB = "11e09eb8-3f76-80e7-8fac-e8d0bb538fb0"
    DATA_FILE = "/tmp/weekly_habits.jsonl"

    @task(
        task_id="load_file_to_bq",
        outlets=[weekly_habits_dataset],
    )
    def load_weekly_data_file(rows: int, params: dict, outlet_events=None):
        if rows > 0:
            job_state = load_file_to_bq(
                conn_id=BQ_CONN_ID,
                file_path=DATA_FILE,
                table_id=f"{params['raw_dataset']}.{BQ_WEEKLY_TABLE}",
            )
            # Update outlet dataset extras
            outlet_events[weekly_habits_dataset].extra = {
                "state": job_state,
                "rows": rows,
            }

    # Define task flow - task functions must be called ()
    weekly_habits = get_notion_habits(
        conn_id=NOTION_CONN_ID,
        database_name="Weekly Disciplines",
        properties=[
            "Name",
            "Date",
            "Church",
            "Fast",
            "Time Prayed",
        ],
        file_path=DATA_FILE,
    )
    load_weekly_data_file(weekly_habits)


# Call DAGs to add to globals
daily_dag = NotionDailyHabits()
weekly_dag = NotionWeeklyHabits()
