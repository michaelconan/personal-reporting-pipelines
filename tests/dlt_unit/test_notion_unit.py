# base imports
import re
import json
from typing import Callable

# PyPI imports
import pytest
from pytest import MonkeyPatch
import dlt
import pytest_responses
from responses import RequestsMock

# local imports
from pipelines.notion import notion_source
from tests.dlt_unit.conftest import sample_data, sample_response


pytestmark = pytest.mark.local


@pytest.fixture
def mock_notion_apis(monkeypatch: MonkeyPatch, responses: RequestsMock) -> Callable:

    BASE_URL = "https://api.notion.com/v1"

    # Mock the API key to prevent credential errors
    monkeypatch.setenv("SOURCES__NOTION__API_KEY", "dummy_api_key")

    def cursor_callback(request):
        """Handle cursor-based pagination for Notion database queries."""
        body = json.loads(request.body)
        start_cursor = body.get("start_cursor")
        after_date = body["filter"]["date"]["after"]

        if "2024-06-05" in after_date:
            # Subsequent run page
            return sample_response("notion_database_rows_run2.json")
        else:
            if not start_cursor:
                # First page
                return sample_response("notion_database_rows_run1-page1.json")
            elif start_cursor == "cursor_page2_token":
                # Second page
                return sample_response("notion_database_rows_run1-page2.json")
        # No more data
        return (
            200,
            {},
            json.dumps(
                {
                    "object": "list",
                    "results": [],
                    "next_cursor": None,
                    "has_more": False,
                }
            ),
        )

    def databases_callback(request):
        """Handle database search requests (single page)."""
        return sample_response("notion_databases.json")

    def setup(endpoints=[]):
        """Nested function to only register mock endpoints for tests.

        Args:
            endpoints (list, optional): Specific endpoints to register. Defaults to [] (all).
        """
        # Mock the API responses
        if not endpoints or "databases" in endpoints:
            responses.add_callback(
                responses.POST,
                BASE_URL + "/search",
                callback=databases_callback,
                content_type="application/json",
            )
        if not endpoints or "database_rows" in endpoints:
            responses.add_callback(
                responses.POST,
                re.compile(BASE_URL + r"/databases/([a-f0-9\-]+)/query"),
                callback=cursor_callback,
                content_type="application/json",
            )

    return setup


class TestNotionPhases:

    def test_extract(
        self,
        mock_notion_apis,
        duckdb_pipeline: dlt.Pipeline,
    ):

        # GIVEN
        # Mocked APIs
        mock_notion_apis()

        # WHEN
        # For Notion, we need to specify which database to query
        source = notion_source(db_name="daily_habits")
        info = duckdb_pipeline.extract(source)

        # THEN
        assert len(info.loads_ids) == 1

    @pytest.mark.parametrize(
        ("resource", "expected_tables", "configs"),
        (
            ("databases", 2, {}),
            ("database_rows", 2, {}),
        ),
    )
    def test_normalize(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):

        # GIVEN
        file_name = f"notion_{resource}_run1-page1.json"
        file_name2 = f"notion_{resource}.json"
        source = sample_data(file_name, fallback=file_name2)

        if "results" in source:
            source = source["results"]
        expected_rows = len(source)

        duckdb_pipeline.extract(source, table_name=resource, **configs)

        # WHEN
        info = duckdb_pipeline.normalize()

        # THEN
        # _dlt_state table, source table, and any child tables
        assert (
            len([r for r in info.row_counts if r.startswith(resource)])
            == expected_tables
        )
        # record count from sample data
        assert info.row_counts[resource] == expected_rows

    @pytest.mark.parametrize(
        ("resource", "configs"),
        (
            ("databases", {}),
            ("database_rows", {}),
        ),
    )
    def test_load(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        configs: dict | None,
    ):
        # GIVEN
        file_name = f"notion_{resource}_run1-page1.json"
        file_name2 = f"notion_{resource}.json"
        source = sample_data(file_name, fallback=file_name2)

        if "results" in source:
            source = source["results"]

        duckdb_pipeline.extract(source, table_name=resource, **configs)
        duckdb_pipeline.normalize()

        # WHEN
        info = duckdb_pipeline.load()

        # THEN
        assert info.has_failed_jobs is False
        assert all(p.state == "loaded" for p in info.load_packages)


@pytest.mark.parametrize(
    ("increment"),
    (
        True,
        False,
    ),
)
def test_notion_pipeline_refresh(mock_notion_apis, duckdb_pipeline, increment: bool):
    """Test that the Notion pipeline runs and loads data correctly.

    Run incremental refresh testing and all source resources together due to resource
    interdependency in Notion data source.
    """
    # GIVEN (1)
    mock_notion_apis()
    expected_rows = 5
    # Defaults to append for most resources
    write_disposition = None if increment else "replace"

    # WHEN (1)
    # Run the pipeline
    source = notion_source(db_name="daily_habits")
    info = duckdb_pipeline.run(source)

    # THEN (1)
    # Assert that the pipeline ran successfully
    assert info.first_run is True
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    dataset = duckdb_pipeline.dataset_name
    with duckdb_pipeline.sql_client() as client:
        # Check databases table (search results)
        databases_table = client.execute_sql(
            f"SELECT 1 FROM {dataset}.notion__databases"
        )
        assert len(databases_table) == 1

        # Check database_rows table
        rows_table = client.execute_sql(
            f"SELECT 1 FROM {dataset}.notion__database_daily_habits"
        )
        assert len(rows_table) == expected_rows

    # GIVEN (2)
    # Only database_rows should increment (databases search results stay same)
    expected_rows += 1 if increment else 0

    # WHEN (2)
    info2 = duckdb_pipeline.run(source, write_disposition=write_disposition)

    # THEN (2)
    # Run pipeline again
    assert info2.first_run is False
    assert info2.has_failed_jobs is False
    # Validate loaded data from incremental run
    with duckdb_pipeline.sql_client() as client:
        # Check database_rows table
        rows_table2 = client.execute_sql(
            f"SELECT 1 FROM {dataset}.notion__database_daily_habits"
        )
        assert len(rows_table2) == expected_rows
