# Base imports

# PyPI imports

# Airflow imports
from airflow.models.dagbag import DagBag
from airflow.utils.state import TaskInstanceState

# Local imports
from tests.conftest import run_dag, DBT_TEST_DATASET


DBT_DAG = "run_dbt"


def test_run_dbt(dag_bag: DagBag):

    # GIVEN
    # Get DAG from DagBag to set context
    dag = dag_bag.get_dag(dag_id=DBT_DAG)

    # WHEN
    # Run the DAG tasks
    tis = run_dag(dag, extras={"conf": {"dataset": DBT_TEST_DATASET}})

    # THEN
    # Validate task instances were successful
    assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis)
