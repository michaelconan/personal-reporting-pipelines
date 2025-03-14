# General imports
import os
import datetime
import pendulum
import pytest

# Airflow imports
from airflow.models.dagbag import DagBag
from airflow.utils.state import TaskInstanceState

# Local imports
from tests.conftest import run_dag, RAW_TEST_SCHEMA


# Set environment variable for testing
os.environ["TEST"] = "True"


@pytest.mark.parametrize(
    ("dag_id", "time_period"),
    (
        ("raw_hubspot__contacts__full", None),
        ("raw_hubspot__companies__full", None),
        ("raw_hubspot__engagements__full", None),
        ("raw_hubspot__contacts__changed", 30),
        ("raw_hubspot__companies__changed", 30),
        ("raw_hubspot__engagements__changed", 30),
    ),
)
def test_hubspot_load(dag_bag: DagBag, dag_id: str, time_period: int):

    # GIVEN
    # Define interval and DAG run parameters
    DATA_INTERVAL_START = pendulum.datetime(2025, 2, 14, tz="UTC")

    # Get DAG from DagBag to set context
    dag = dag_bag.get_dag(dag_id=dag_id)
    args = {
        "execution_date": DATA_INTERVAL_START,
        "conf": {"raw_schema": RAW_TEST_SCHEMA},
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
