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

# Separate schema to run tests
ADMIN_TEST_SCHEMA = "test_admin"
RAW_TEST_SCHEMA = "test_raw"
DBT_TEST_TARGET = "test"


@pytest.fixture
def dag_bag():
    # Set schema variables
    os.environ["ADMIN_SCHEMA"] = ADMIN_TEST_SCHEMA
    os.environ["RAW_SCHEMA"] = RAW_TEST_SCHEMA
    os.environ["DBT_TARGET"] = DBT_TEST_TARGET
    return DagBag(dag_folder="./dags", include_examples=False)


def get_instances(run: DagRun):
    instances = run.get_task_instances()
    task_instance_dict = dict()
    for instance in instances:
        instance.task = run.dag.get_task(instance.task_id)
        task_instance_dict[instance.task_id] = instance

    ordered_tasks = list()
    assigned_tasks = set()
    remaining_tasks = set(task_instance_dict.keys())
    while remaining_tasks:
        for task_id in list(remaining_tasks):
            task_instance = task_instance_dict[task_id]
            if task_instance.task.upstream_task_ids.issubset(assigned_tasks):
                assigned_tasks.add(task_id)
                ordered_tasks.append(task_instance)
                remaining_tasks.remove(task_id)
                break

    return ordered_tasks


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
