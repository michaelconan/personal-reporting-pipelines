import pytest
import dlt
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
    source = hubspot_source()
    list(source)

    # Assert that rest_api_resources was called with the expected config
    mock_rest_api_resources.assert_called_once()
    call_args = mock_rest_api_resources.call_args[0][0]

    # Check some key parts of the config
    assert call_args["client"]["base_url"] == "https://api.hubapi.com/"
    assert len(call_args["resources"]) == 5

    # Check contacts resource
    contacts_resource = next(r for r in call_args["resources"] if r["name"] == "hubspot__contacts")
    assert "incremental" in contacts_resource["endpoint"]
    assert "json" in contacts_resource["endpoint"]
    assert "filterGroups" in contacts_resource["endpoint"]["json"]

    # Check engagements resource
    engagements_resource = next(r for r in call_args["resources"] if r["name"] == "hubspot__engagements")
    assert "incremental" in engagements_resource["endpoint"]
    assert "json" in engagements_resource["endpoint"]
    assert "filterGroups" in engagements_resource["endpoint"]["json"]
