# General imports
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
        ("NotionDailyHabits", 1),
        ("NotionWeeklyHabits", 7),
    ),
)
def test_1_notion_habits(dag_bag: DagBag, dag_id: str, time_period: int):

    # GIVEN
    # Define interval and DAG run parameters
    DATA_INTERVAL_START = pendulum.datetime(2024, 12, 14, tz="UTC")
    DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=time_period)
    BQ_TEST_DATASET = "raw"

    # Get DAG from DagBag to set context
    dag = dag_bag.get_dag(dag_id=dag_id)
    args = {
        "execution_date": DATA_INTERVAL_START,
        "data_interval": (DATA_INTERVAL_START, DATA_INTERVAL_END),
        "conf": {"raw_dataset": BQ_TEST_DATASET},
    }

    # WHEN
    # Run the DAG
    tis = run_dag(dag, extras=args)

    # THEN
    # Validate task instances were successful
    assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis)
