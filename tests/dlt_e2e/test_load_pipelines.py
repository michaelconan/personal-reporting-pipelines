# e2e tests for pipelines
# NOTE: these tests require credentials to run
# and will be skipped by default
import pytest
import pendulum
import dlt


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


@pytest.mark.e2e
def test_notion_load(duckdb_pipeline, mocker):

    # Delayed import to capture DBT target variable
    from pipelines.notion import refresh_notion

    # GIVEN
    mocker.patch.dict(os.environ, {"NOTION_TOKEN": "dummy_notion_token"})

    # WHEN
    # Run the pipeline
    refresh_notion(is_incremental=False, pipeline=duckdb_pipeline)

    # THEN
    # Validate task instances were successful


@pytest.mark.e2e
def test_hubspot_full_refresh():
    """
    Test a full refresh of the HubSpot pipeline.
    """
    from pipelines.hubspot import refresh_hubspot

    print("Running test_hubspot_full_refresh...")
    info = refresh_hubspot(is_incremental=False)
    print(info)
    # Add assertions here to validate the data
    assert info.status == "completed"


@pytest.mark.e2e
def test_hubspot_incremental_load():
    """
    Test an incremental load of the HubSpot pipeline.
    """
    from pipelines.hubspot import refresh_hubspot

    # First, run a full refresh to have some data
    print("Running initial full refresh for incremental test...")
    refresh_hubspot(is_incremental=False)

    # Now, run an incremental load
    print("Running test_hubspot_incremental_load...")
    info = refresh_hubspot(is_incremental=True)

    # Add assertions here to validate the data
    assert info.status == "completed"


@pytest.mark.e2e
def test_fitbit_load():

    # Delayed import to capture DBT target variable
    from pipelines.fitbit import refresh_fitbit

    # GIVEN

    # WHEN
    # Run the DAG
    info = refresh_fitbit(is_incremental=False)

    # THEN
    print(info)
    assert info.status == "completed"
