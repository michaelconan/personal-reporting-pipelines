import uuid
import datetime
import pendulum

from airflow import DAG
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from dags.notion_habits import dag


def test_notion_habits():

    # GIVEN
    # Define interval and DAG run parameters
    DATA_INTERVAL_START = pendulum.datetime(2024, 10, 15, tz="UTC")
    DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)
    BQ_TEST_TABLE = "test_notion_habits_daily"
    dag.dag_id = f"{dag.dag_id}-{uuid.uuid4()}"
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
        # Override params to use test table
        conf={"daily_habit_table": BQ_TEST_TABLE},
    )

    # WHEN
    # Run the DAG tasks
    task_instances = dagrun.get_task_instances()
    tis = list()
    for ti in dagrun.get_task_instances():
        ti.task = dag.get_task(ti.task_id)
        ti.run(ignore_ti_state=True, ignore_all_deps=True)
        tis.append(ti)

    # THEN
    # Validate task instances were successful
    assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis)
