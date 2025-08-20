# e2e tests for pipelines
# NOTE: these tests require credentials to run
# and will be skipped by default
import pytest
import pendulum
import dlt


pytestmark = pytest.mark.e2e


@pytest.fixture(scope="function")
def duckdb_pipeline():
    """
    Fixture to create and tear down a duckdb pipeline.
    """
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline",
        dataset_name="test_data",
        destination="duckdb",
    )
    yield pipeline
    pipeline.drop()


def test_notion_refresh(duckdb_pipeline, mocker):

    # Delayed import to capture DBT target variable
    from pipelines.notion import refresh_notion

    # GIVEN

    # WHEN
    # Run the pipeline
    refresh_notion(is_incremental=False)

    # THEN
    # Validate task instances were successful


def test_hubspot_refresh():
    """
    Test a full refresh of the HubSpot pipeline.
    """
    from pipelines.hubspot import refresh_hubspot

    print("Running test_hubspot_full_refresh...")
    info = refresh_hubspot(is_incremental=False)
    print(info)
    # Add assertions here to validate the data
    assert info.status == "completed"


def test_fitbit_refresh():

    # Delayed import to capture DBT target variable
    from pipelines.fitbit import refresh_fitbit

    # GIVEN

    # WHEN
    # Run the DAG
    info = refresh_fitbit(is_incremental=False)

    # THEN
    print(info)
    assert info.status == "completed"
