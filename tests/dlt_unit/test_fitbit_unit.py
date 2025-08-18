import pytest
import dlt
import os
import json
import responses
from urllib.parse import parse_qs, urlparse

from pipelines.fitbit import fitbit_source


def fitbit_sleep_callback(request):
    """
    Callback function to handle pagination for Fitbit sleep API.
    Returns data only for offset=0, empty response for other offsets.
    """
    parsed_url = urlparse(request.url)
    params = parse_qs(parsed_url.query)
    offset = int(params.get("offset", [0])[0])

    if offset == 0:
        # Return actual data for first page
        with open("/fitbit_sleep.json", "r") as f:
            return (200, {}, json.dumps(json.load(f)))
    else:
        # Return empty page for subsequent requests
        return (200, {}, json.dumps({"sleep": []}))


def fitbit_activities_callback(request):
    """
    Callback function to handle pagination for Fitbit activities API.
    Returns data only for offset=0, empty response for other offsets.
    """
    parsed_url = urlparse(request.url)
    params = parse_qs(parsed_url.query)
    offset = int(params.get("offset", [0])[0])

    if offset == 0:
        # Return actual data for first page
        with open("tests/mock_data/fitbit_activities.json", "r") as f:
            return (200, {}, json.dumps(json.load(f)))
    else:
        # Return empty page for subsequent requests
        return (200, {}, json.dumps({"activities": []}))


@responses.activate
def test_fitbit_pipeline(monkeypatch, mocker):
    """
    Test that the Fitbit pipeline runs and loads data correctly.
    """
    # Mock the API responses with pagination logic
    responses.add_callback(
        responses.GET,
        "https://api.fitbit.com/1.2/user/-/sleep/list.json",
        callback=fitbit_sleep_callback,
        content_type="application/json",
    )
    responses.add_callback(
        responses.GET,
        "https://api.fitbit.com/1/user/-/activities/list.json",
        callback=fitbit_activities_callback,
        content_type="application/json",
    )

    # mock the get_fitbit_token function to prevent real API calls
    mocker.patch("pipelines.fitbit.get_fitbit_token", return_value="dummy_token")

    # Define the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="fitbit_unit_test",
        destination="duckdb",
        dataset_name="fitbit_data",
        dev_mode=True,
    )

    # Run the pipeline
    source = fitbit_source(api_key="dummy_token")
    info = pipeline.run(source)

    # Assert that the pipeline ran successfully
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    with pipeline.sql_client() as client:
        # Check sleep table
        sleep_table = client.execute_sql(
            "SELECT log_id, date_of_sleep FROM fitbit__sleep"
        )
        assert len(sleep_table) == 2
        assert sleep_table[0][0] == 1
        assert sleep_table[0][1] == "2024-01-01"

        # Check activities table
        activities_table = client.execute_sql(
            "SELECT log_id, activity_name FROM fitbit__activities"
        )
        assert len(activities_table) == 2
        assert activities_table[0][0] == 1
        assert activities_table[0][1] == "Walking"
