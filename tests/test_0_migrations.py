# Airflow imports
from airflow.models.dagbag import DagBag
from airflow.utils.state import TaskInstanceState

# Local imports
from tests.conftest import run_dag


def test_1_downgrade(dag_bag: DagBag):

    # GIVEN
    # Get DAG from DagBag to set context
    dag = dag_bag.get_dag(dag_id="raw_migrations")
    args = {
        "conf": {"command": "downgrade", "revision": "base"},
    }

    # WHEN
    # Run the DAG tasks
    tis = run_dag(dag, extras=args)

    # THEN
    # Validate task instances were successful
    assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis)


def test_2_upgrade(dag_bag: DagBag):

    # GIVEN
    # Get DAG from DagBag to set context
    dag = dag_bag.get_dag(dag_id="raw_migrations")
    args = {
        "conf": {"command": "upgrade", "revision": "head"},
    }

    # WHEN
    # Run the DAG tasks
    tis = run_dag(dag, extras=args)

    # THEN
    # Validate task instances were successful
    assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis)
