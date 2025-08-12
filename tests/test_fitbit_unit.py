import pytest
import dlt
import os

from pipelines.fitbit import fitbit_source


def test_fitbit_source_config(monkeypatch, mocker):
    """
    Test that the fitbit_source function generates the correct API configuration.
    """
    # mock the get_fitbit_token function to prevent real API calls
    mocker.patch("pipelines.fitbit.get_fitbit_token", return_value="dummy_token")
    monkeypatch.setenv("SOURCES__FITBIT__CLIENT_ID", "dummy_client_id")
    monkeypatch.setenv("SOURCES__FITBIT__CLIENT_SECRET", "dummy_client_secret")
    monkeypatch.setenv("SOURCES__FITBIT__REFRESH_TOKEN", "dummy_refresh_token")
    mock_rest_api_resources = mocker.patch(
        "pipelines.fitbit.rest_api_source", return_value=iter([])
    )

    # The fitbit_source is a generator that yields the resources
    source = fitbit_source(api_key="test_key")
    list(source)

    # Assert that rest_api_resources was called with the expected config
    mock_rest_api_resources.assert_called_once()
    call_args = mock_rest_api_resources.call_args[0][0]

    # Check some key parts of the config
    assert call_args["client"]["base_url"] == "https://api.fitbit.com/"
    assert len(call_args["resources"]) == 2
    sleep_resource = next(r for r in call_args["resources"] if r["name"] == "fitbit__sleep")
    assert "incremental" in sleep_resource["endpoint"]
    assert "afterDate" in sleep_resource["endpoint"]["params"]
    activities_resource = next(r for r in call_args["resources"] if r["name"] == "fitbit__activities")
    assert "incremental" in activities_resource["endpoint"]
    assert "afterDate" in activities_resource["endpoint"]["params"]
