# Base imports
import os

# PyPI imports
import pytest

# Airflow imports
from airflow.models.dagbag import DagBag
from airflow.utils.state import TaskInstanceState

# Local imports
from tests.conftest import run_dag


MIGRATE_DAG = "migrate_raw_tables"
os.environ["VERSION_TABLE"] = "test_alembic_version"


@pytest.mark.parametrize(
    ("command", "revision"),
    (
        ("downgrade", "base"),
        ("upgrade", "head"),
    ),
)
def test_migration(dag_bag: DagBag, command: str, revision: str):

    # GIVEN
    # Get DAG from DagBag to set context
    dag = dag_bag.get_dag(dag_id=MIGRATE_DAG)
    args = {
        "conf": {"command": command, "revision": revision},
    }

    # WHEN
    # Run the DAG tasks
    tis = run_dag(dag, extras=args)

    # THEN
    # Validate task instances were successful
    assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis)
