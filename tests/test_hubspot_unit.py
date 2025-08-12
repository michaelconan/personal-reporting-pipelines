import pytest
import dlt
import pendulum
import os

from pipelines.hubspot import hubspot_source


def test_hubspot_source_config(monkeypatch, mocker):
    """
    Test that the hubspot_source function generates the correct API configuration.
    """
    # mock the dlt.secrets.value to prevent credential errors
    monkeypatch.setenv("SOURCES__HUBSPOT__API_KEY", "dummy_api_key")
    mock_rest_api_resources = mocker.patch(
        "pipelines.hubspot.rest_api_resources", return_value=iter([])
    )

    # The hubspot_source is a generator that yields the resources
    source = hubspot_source(is_incremental=False)
    list(source)

    # Assert that rest_api_resources was called with the expected config
    mock_rest_api_resources.assert_called_once()
    call_args = mock_rest_api_resources.call_args[0][0]

    # Check some key parts of the config
    assert call_args["client"]["base_url"] == "https://api.hubapi.com/"
    # The number of resources is 5 in full refresh
    assert len(call_args["resources"]) == 5
    contacts_resource = next(r for r in call_args["resources"] if r["name"] == "hubspot__contacts")
    assert contacts_resource["endpoint"]["path"] == "crm/v3/objects/contacts/search"


def test_hubspot_source_incremental_config(monkeypatch, mocker):
    """
    Test that the hubspot_source function generates the correct API configuration for incremental loads.
    """
    # mock the dlt.secrets.value to prevent credential errors
    monkeypatch.setenv("SOURCES__HUBSPOT__API_KEY", "dummy_api_key")
    mock_rest_api_resources = mocker.patch(
        "pipelines.hubspot.rest_api_resources", return_value=iter([])
    )

    # The hubspot_source is a generator that yields the resources
    initial_date = pendulum.now().subtract(days=10).to_iso8601_string()
    source = hubspot_source(is_incremental=True, initial_date=initial_date)
    list(source)

    # Assert that rest_api_resources was called with the expected config
    mock_rest_api_resources.assert_called_once()
    call_args = mock_rest_api_resources.call_args[0][0]

    # Check some key parts of the config
    assert call_args["client"]["base_url"] == "https://api.hubapi.com/"
    # The number of resources is 3 in incremental mode
    assert len(call_args["resources"]) == 3
    contacts_resource = next(r for r in call_args["resources"] if r["name"] == "hubspot__contacts")
    assert contacts_resource["endpoint"]["path"] == "crm/v3/objects/contacts/search"
    assert "incremental" in contacts_resource["endpoint"]
