import pytest
import dlt
import os

from pipelines.notion import notion_source


def test_notion_source_config(monkeypatch, mocker):
    """
    Test that the notion_source function generates the correct API configuration.
    """
    # mock the dlt.secrets.value to prevent credential errors
    monkeypatch.setenv("SOURCES__NOTION__API_KEY", "dummy_notion_token")
    mock_rest_api_resources = mocker.patch(
        "pipelines.notion.rest_api_resources", return_value=iter([])
    )

    # The notion_source is a generator that yields the resources
    source = notion_source(db_name="test_db")
    list(source)

    # Assert that rest_api_resources was called with the expected config
    mock_rest_api_resources.assert_called_once()
    call_args = mock_rest_api_resources.call_args[0][0]

    # Check some key parts of the config
    assert call_args["client"]["base_url"] == "https://api.notion.com/v1"
    assert len(call_args["resources"]) == 2
    rows_resource = next(r for r in call_args["resources"] if r["name"] == "notion__database_rows")
    assert "incremental" in rows_resource["endpoint"]
    assert "json" in rows_resource["endpoint"]
    assert "filter" in rows_resource["endpoint"]["json"]
