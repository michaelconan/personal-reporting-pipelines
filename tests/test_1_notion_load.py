# General imports
import datetime
import pendulum
import pytest

# Airflow imports
from airflow.models.dagbag import DagBag
from airflow.utils.state import TaskInstanceState

# Local imports
# NOTE: import of dags.michael is delayed to update environment variables
from tests.conftest import run_dag


@pytest.mark.parametrize(
    ("dag_id", "time_period"),
    (
        ("raw_notion__daily_habits__full", None),
        ("raw_notion__weekly_habits__full", None),
        ("raw_notion__monthly_habits__full", None),
        ("raw_notion__daily_habits__changed", 1),
        ("raw_notion__weekly_habits__changed", 7),
        ("raw_notion__monthly_habits__changed", 31),
    ),
)
def test_notion_habits(dag_bag: DagBag, dag_id: str, time_period: int):

    # Delayed import to capture DBT target variable
    from dags.michael import RAW_SCHEMA

    # GIVEN
    # Define interval and DAG run parameters
    DATA_INTERVAL_START = pendulum.datetime(2024, 12, 14, tz="UTC")

    # Get DAG from DagBag to set context
    dag = dag_bag.get_dag(dag_id=dag_id)
    args = {
        "execution_date": DATA_INTERVAL_START,
        "conf": {"raw_schema": RAW_SCHEMA},
    }
    if time_period:
        DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=time_period)
        args["data_interval"] = (DATA_INTERVAL_START, DATA_INTERVAL_END)

    # WHEN
    # Run the DAG
    tis = run_dag(dag, extras=args)

    # THEN
    # Validate task instances were successful
    assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis)


def test_notion_hints():

    from dags.michael.dlt_notion__habits import get_property_schema

    # GIVEN
    # Sample page data from Notion API
    page = {
        "object": "page",
        "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
        "created_time": "2022-03-01T19:05:00.000Z",
        "last_edited_time": "2022-07-06T20:25:00.000Z",
        "created_by": {"object": "user", "id": "ee5f0f84-409a-440f-983a-a5315961c6e4"},
        "last_edited_by": {
            "object": "user",
            "id": "0c3e9826-b8f7-4f73-927d-2caaf86f1103",
        },
        "parent": {
            "type": "database_id",
            "database_id": "d9824bdc-8445-4327-be8b-5b47500af6ce",
        },
        "archived": False,
        "properties": {
            "Store availability": {
                "id": "%3AUPp",
                "type": "multi_select",
                "multi_select": [
                    {"id": "t|O@", "name": "Gus's Community Market", "color": "yellow"},
                    {"id": "{Ml\\", "name": "Rainbow Grocery", "color": "gray"},
                ],
            },
            "Food group": {
                "id": "A%40Hk",
                "type": "select",
                "select": {
                    "id": "5e8e7e8f-432e-4d8a-8166-1821e10225fc",
                    "name": "🥬 Vegetable",
                    "color": "pink",
                },
            },
            "Price": {"id": "BJXS", "type": "number", "number": 2.5},
            "Last ordered": {
                "id": "Jsfb",
                "type": "date",
                "date": {"start": "2022-02-22", "end": None, "time_zone": None},
            },
            "Cost of next trip": {
                "id": "WOd%3B",
                "type": "formula",
                "formula": {"type": "number", "number": 0},
            },
            "Recipes": {
                "id": "YfIu",
                "type": "relation",
                "relation": [
                    {"id": "90eeeed8-2cdd-4af4-9cc1-3d24aff5f63c"},
                    {"id": "a2da43ee-d43c-4285-8ae2-6d811f12629a"},
                ],
                "has_more": False,
            },
            "Name": {
                "id": "title",
                "type": "title",
                "title": [
                    {
                        "type": "text",
                        "text": {"content": "Tuscan kale", "link": None},
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default",
                        },
                        "plain_text": "Tuscan kale",
                        "href": None,
                    }
                ],
            },
        },
        "url": "https://www.notion.so/Tuscan-kale-598337872cf94fdf8782e53db20768a5",
    }

    # WHEN
    # Get the property schema hints from the page
    hints = get_property_schema(page)

    # THEN
    # Validate the hints contain expected properties
    print(hints)


@pytest.mark.parametrize(
    ("dag_id", "time_period"),
    (("raw_notion__habits__changed", 31),),
)
def test_notion_dlt(dag_bag: DagBag, dag_id: str, time_period: int):

    # Delayed import to capture DBT target variable
    from dags.michael import RAW_SCHEMA

    # GIVEN
    # Define interval and DAG run parameters
    DATA_INTERVAL_START = pendulum.datetime(2024, 12, 14, tz="UTC")

    # Get DAG from DagBag to set context
    dag = dag_bag.get_dag(dag_id=dag_id)
    args = {
        "execution_date": DATA_INTERVAL_START,
        "conf": {"raw_schema": RAW_SCHEMA},
    }
    if time_period:
        DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=time_period)
        args["data_interval"] = (DATA_INTERVAL_START, DATA_INTERVAL_END)

    # WHEN
    # Run the DAG
    tis = run_dag(dag, extras=args)

    # THEN
    # Validate task instances were successful
    assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis)
