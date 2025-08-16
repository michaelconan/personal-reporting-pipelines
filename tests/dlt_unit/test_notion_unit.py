import pytest
import dlt
import os
import json
import responses

from pipelines.notion import notion_source

@responses.activate
def test_notion_pipeline(monkeypatch):
    """
    Test that the Notion pipeline runs and loads data correctly.
    """
    # Mock the API responses
    with open("tests/mock_data/notion_database_rows.json", "r") as f:
        responses.add(
            responses.POST,
            "https://api.notion.com/v1/databases/10140b50-0f0d-43d2-905a-7ed714ef7f2c/query",
            json=json.load(f),
            status=200,
        )

    # mock the dlt.secrets.value to prevent credential errors
    monkeypatch.setenv("SOURCES__NOTION__API_KEY", "dummy_notion_token")

    # Define the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="notion_unit_test",
        destination="duckdb",
        dataset_name="notion_data",
    )

    # Run the pipeline
    source = notion_source(db_id="10140b50-0f0d-43d2-905a-7ed714ef7f2c")
    info = pipeline.run(source)

    # Assert that the pipeline ran successfully
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    with pipeline.sql_client() as client:
        # Check database rows table
        rows_table = client.execute_sql("SELECT id FROM notion_data.notion__database_rows")
        assert len(rows_table) == 2
        assert rows_table[0][0] == "59833787-2cf9-4fdf-8782-e53db20768a5"
