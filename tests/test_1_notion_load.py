# General imports
import datetime
import pendulum
import pytest

# Airflow imports
from airflow.models.dagbag import DagBag
from airflow.utils.state import TaskInstanceState

# Local imports
from tests.conftest import run_dag, BQ_TEST_DATASET


@pytest.mark.parametrize(
    ("dag_id", "time_period"),
    (
        ("load_initial_daily_habits", None),
        ("load_initial_weekly_habits", None),
        ("load_changed_daily_habits", 1),
        ("load_changed_weekly_habits", 7),
    ),
)
def test_1_notion_habits(dag_bag: DagBag, dag_id: str, time_period: int):

    # GIVEN
    # Define interval and DAG run parameters
    DATA_INTERVAL_START = pendulum.datetime(2024, 12, 14, tz="UTC")

    # Get DAG from DagBag to set context
    dag = dag_bag.get_dag(dag_id=dag_id)
    args = {
        "execution_date": DATA_INTERVAL_START,
        "conf": {"raw_dataset": BQ_TEST_DATASET},
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
