# # Base imports

# # PyPI imports
# import pytest

# # Airflow imports
# from airflow.models.dagbag import DagBag
# from airflow.utils.state import TaskInstanceState

# # Local imports
# from tests.conftest import run_dag, DBT_TEST_TARGET


# @pytest.mark.parametrize("dag_id", ["dbt__michael", "dbt_docs__michael"])
# def test_run_dbt(dag_bag: DagBag, dag_id: str):

#     # GIVEN
#     # Get DAG from DagBag to set context
#     dag = dag_bag.get_dag(dag_id=dag_id)

#     # WHEN
#     # Run the DAG tasks
#     tis = run_dag(dag, extras={"conf": {"target": DBT_TEST_TARGET}})

#     # THEN
#     # Validate task instances were successful
#     assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis)
