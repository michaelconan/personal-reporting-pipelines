import pytest
import dlt
import re
import json
import responses

from pipelines.notion import notion_source


@responses.activate
def test_notion_pipeline(monkeypatch):
    """
    Test that the Notion pipeline runs and loads data correctly.
    """
    # GIVEN
    # Mock the API responses
    with open("tests/mock_data/notion_search.json", "r") as f:
        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=json.load(f),
            status=200,
        )

    with open("tests/mock_data/notion_database_rows.json", "r") as f:
        responses.add(
            responses.POST,
            re.compile(r"https://api.notion.com/v1/databases/([\w\-]+)/query"),
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
        dev_mode=True,
    )

    # WHEN
    # Run the pipeline
    source = notion_source(db_name="Disciplines")
    info = pipeline.run(source)

    # THEN
    # Assert that the pipeline ran successfully
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    with pipeline.sql_client() as client:
        # Check database rows table
        table_names = client.execute_sql(
            f"""SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{pipeline.dataset_name}'
            """
        )
        rows_table_name = [t[0] for t in table_names if "database_" in t[0]]
        rows_table = client.execute_sql(
            f"SELECT id FROM {pipeline.dataset_name}.{rows_table_name[0]}"
        )
        assert len(rows_table) == 2
        assert rows_table[0][0] == "59833787-2cf9-4fdf-8782-e53db20768a5"
