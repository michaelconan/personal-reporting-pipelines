# e2e tests for pipelines
# NOTE: these tests require credentials to run
# and will be skipped by default
import pytest
import pendulum
import dlt


# Sample page data from Notion API
PAGE = {
    "object": "page",
    "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
    "created_time": "2022-03-01T19:05:00.000Z",
    "last_edited_time": "2022-07-06T20:25:00.000Z",
    "created_by": {"object": "user", "id": "ee5f0f84-409a-440f-983a-a5315961c6e4"},
    "last_edited_by": {
        "object": "user",
        "id": "0c3e9826-b8f7-4f73-927d-2caaf86f1103",
    },
    "parent": {
        "type": "database_id",
        "database_id": "d9824bdc-8445-4327-be8b-5b47500af6ce",
    },
    "archived": False,
    "properties": {
        "Store availability": {
            "id": "%3AUPp",
            "type": "multi_select",
            "multi_select": [
                {"id": "t|O@", "name": "Gus's Community Market", "color": "yellow"},
                {"id": "{Ml\\", "name": "Rainbow Grocery", "color": "gray"},
            ],
        },
        "Food group": {
            "id": "A%40Hk",
            "type": "select",
            "select": {
                "id": "5e8e7e8f-432e-4d8a-8166-1821e10225fc",
                "name": "🥬 Vegetable",
                "color": "pink",
            },
        },
        "Price": {"id": "BJXS", "type": "number", "number": 2.5},
        "Last ordered": {
            "id": "Jsfb",
            "type": "date",
            "date": {"start": "2022-02-22", "end": None, "time_zone": None},
        },
        "Cost of next trip": {
            "id": "WOd%3B",
            "type": "formula",
            "formula": {"type": "number", "number": 0},
        },
        "Recipes": {
            "id": "YfIu",
            "type": "relation",
            "relation": [
                {"id": "90eeeed8-2cdd-4af4-9cc1-3d24aff5f63c"},
                {"id": "a2da43ee-d43c-4285-8ae2-6d811f12629a"},
            ],
            "has_more": False,
        },
        "Name": {
            "id": "title",
            "type": "title",
            "title": [
                {
                    "type": "text",
                    "text": {"content": "Tuscan kale", "link": None},
                    "annotations": {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "code": False,
                        "color": "default",
                    },
                    "plain_text": "Tuscan kale",
                    "href": None,
                }
            ],
        },
    },
    "url": "https://www.notion.so/Tuscan-kale-598337872cf94fdf8782e53db20768a5",
}


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
def test_hubspot_full_refresh(duckdb_pipeline, mocker):
    """
    Test a full refresh of the HubSpot pipeline.
    """
    from pipelines.hubspot import refresh_hubspot

    mocker.patch.dict(os.environ, {"SOURCES__HUBSPOT__API_KEY": "dummy_api_key"})
    print("Running test_hubspot_full_refresh...")
    info = refresh_hubspot(is_incremental=False, pipeline=duckdb_pipeline)
    print(info)
    # Add assertions here to validate the data
    assert duckdb_pipeline.last_run_load_info.status == "completed"


@pytest.mark.e2e
def test_hubspot_incremental_load(duckdb_pipeline, mocker):
    """
    Test an incremental load of the HubSpot pipeline.
    """
    from pipelines.hubspot import refresh_hubspot

    mocker.patch.dict(os.environ, {"SOURCES__HUBSPOT__API_KEY": "dummy_api_key"})
    # First, run a full refresh to have some data
    print("Running initial full refresh for incremental test...")
    refresh_hubspot(is_incremental=False, pipeline=duckdb_pipeline)

    # Now, run an incremental load
    print("Running test_hubspot_incremental_load...")
    info = refresh_hubspot(is_incremental=True, pipeline=duckdb_pipeline)
    print(info)
    # Add assertions here to validate the data
    assert duckdb_pipeline.last_run_load_info.status == "completed"


@pytest.mark.e2e
def test_fitbit_load(duckdb_pipeline, mocker):

    # Delayed import to capture DBT target variable
    from pipelines.fitbit import refresh_fitbit

    # GIVEN
    mocker.patch("pipelines.fitbit.get_fitbit_token", return_value="dummy_token")
    mocker.patch.dict(os.environ, {
        "SOURCES__FITBIT__CLIENT_ID": "dummy_client_id",
        "SOURCES__FITBIT__CLIENT_SECRET": "dummy_client_secret",
        "SOURCES__FITBIT__REFRESH_TOKEN": "dummy_refresh_token"
    })


    # WHEN
    # Run the DAG
    info = refresh_fitbit(is_incremental=False, pipeline=duckdb_pipeline)

    # THEN
    print(info)
    assert duckdb_pipeline.last_run_load_info.status == "completed"
