# General imports
import os
import datetime
import pendulum
import pytest

# Airflow imports
from airflow.models.dagbag import DagBag
from airflow.utils.state import TaskInstanceState

# Local imports
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


@pytest.mark.parametrize(
    ("dag_id", "time_period"),
    (("raw_notion__habits", 31),),
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
