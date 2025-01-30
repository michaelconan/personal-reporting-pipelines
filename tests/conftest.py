# Base imports
import uuid
import os

# PyPI imports
import pytest
import pendulum

# Airflow imports
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

# Separate dataset to run tests
RAW_TEST_DATASET = "test_raw"
DBT_TEST_DATASET = "test_reporting"


@pytest.fixture
def dag_bag():
    # Set dataset variables
    os.environ["RAW_SCHEMA"] = RAW_TEST_DATASET
    os.environ["DBT_SCHEMA"] = DBT_TEST_DATASET
    return DagBag(dag_folder="./dags", include_examples=False)


def get_instances(run: DagRun):
    instances = run.get_task_instances()
    for instance in instances:
        instance.task = run.dag.get_task(instance.task_id)
    return sorted(instances, key=lambda ti: ti.task.upstream_task_ids)


def run_dag_tasks(run: DagRun):
    tis = list()
    instances = get_instances(run)
    for ti in instances:
        ti.run(ignore_ti_state=True, ignore_all_deps=True)
        # Append updated (run) task instance to new list
        tis.append(ti)
    return tis


def run_dag(dag: DAG, extras: dict = {}):
    if dag is None:
        raise ValueError("DAG not found")

    # Generate unique ID to avoid conflicts
    dag.dag_id = f"{dag.dag_id}-{uuid.uuid4()}"

    run_args = {
        "state": DagRunState.RUNNING,
        "execution_date": pendulum.now(),
        "run_type": DagRunType.MANUAL,
        **extras,
    }
    dagrun = dag.create_dagrun(**run_args)

    # Run the DAG tasks
    tis = run_dag_tasks(dagrun)

    return tis
