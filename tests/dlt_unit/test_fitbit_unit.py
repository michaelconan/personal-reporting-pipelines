import pytest
import dlt
import os
import json
import responses

from pipelines.fitbit import fitbit_source

@responses.activate
def test_fitbit_pipeline(monkeypatch, mocker):
    """
    Test that the Fitbit pipeline runs and loads data correctly.
    """
    # Mock the API responses
    with open("tests/mock_data/fitbit_sleep.json", "r") as f:
        responses.add(
            responses.GET,
            "https://api.fitbit.com/1.2/user/-/sleep/list.json",
            json=json.load(f),
            status=200,
        )
    with open("tests/mock_data/fitbit_activities.json", "r") as f:
        responses.add(
            responses.GET,
            "https://api.fitbit.com/1/user/-/activities/list.json",
            json=json.load(f),
            status=200,
        )

    # mock the get_fitbit_token function to prevent real API calls
    mocker.patch("pipelines.fitbit.get_fitbit_token", return_value="dummy_token")

    # Define the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="fitbit_unit_test",
        destination="duckdb",
        dataset_name="fitbit_data",
    )

    # Run the pipeline
    source = fitbit_source(api_key="dummy_token")
    info = pipeline.run(source)

    # Assert that the pipeline ran successfully
    assert info.has_failed_jobs is False
    assert info.loads_count == 1

    # Check the loaded data
    with pipeline.sql_client() as client:
        # Check sleep table
        sleep_table = client.sql_query("SELECT log_id, date_of_sleep FROM fitbit_data.fitbit__sleep")
        assert len(sleep_table) == 2
        assert sleep_table[0][0] == 1
        assert sleep_table[0][1] == "2024-01-01"

        # Check activities table
        activities_table = client.sql_query("SELECT log_id, activity_name FROM fitbit_data.fitbit__activities")
        assert len(activities_table) == 2
        assert activities_table[0][0] == 1
        assert activities_table[0][1] == "Walking"
