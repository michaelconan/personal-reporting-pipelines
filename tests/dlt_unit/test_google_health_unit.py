# base imports
import json
from typing import Callable
from urllib.parse import parse_qs, urlparse

# PyPI imports
import pytest
from pytest import MonkeyPatch
import dlt

# local imports
from pipelines.sources.google_health import google_health_source
from tests.dlt_unit.conftest import sample_response, sample_resource


pytestmark = pytest.mark.local


@pytest.fixture
def mock_google_health_apis(monkeypatch: MonkeyPatch, mock_responses) -> Callable:

    BASE_URL = "https://health.googleapis.com"
    mock_responses.assert_all_requests_are_fired = False

    # Mock the token to prevent credential errors
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_ID", "dummy_client_id")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_SECRET", "dummy_secret")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__REFRESH_TOKEN", "dummy_refresh_token")


    def cursor_callback(request, resource: str):
        """Handle cursor-based pagination for Google Health APIs."""
        parsed_url = urlparse(request.url)
        params = parse_qs(parsed_url.query)
        page_token = params.get("pageToken", [None])[0]

        # Use start/end values in filter to distinguish runs if necessary,
        # but standard test checks the pagination flow via page token.
        if "2024-06-05" in parsed_url.query or "2024-06-05" in params.get("filter", [""])[0]:
            # Subsequent run page
            return sample_response(f"google_health_{resource}_run2.json")
        else:
            if not page_token:
                # First page
                return sample_response(f"google_health_{resource}_run1-page1.json")
            elif page_token == "token-page2":
                # Second page
                return sample_response(f"google_health_{resource}_run1-page2.json")
        # No more data
        return (200, {}, json.dumps({"dataPoints": []}))

    def setup(endpoints=[]):
        """Nested function to only register mock endpoints for tests.

        Args:
            endpoints (list, optional): Specific endpoints to register. Defaults to [] (all).
        """
        # Mock the OAuth 2.0 token refresh request
        mock_responses.add(
            mock_responses.POST,
            "https://oauth2.googleapis.com/token",
            json={"access_token": "dummy_access_token", "refresh_token": "new_dummy_refresh_token"},
            status=200,
        )

        # Mock the API responses
        if not endpoints or "sleep" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/v4/users/me/dataTypes/sleep/dataPoints",
                callback=lambda r: cursor_callback(r, "sleep"),
                content_type="application/json",
            )
        if not endpoints or "steps" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/v4/users/me/dataTypes/steps/dataPoints",
                callback=lambda r: cursor_callback(r, "steps"),
                content_type="application/json",
            )
        if not endpoints or "exercise" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/v4/users/me/dataTypes/exercise/dataPoints",
                callback=lambda r: cursor_callback(r, "exercise"),
                content_type="application/json",
            )

    return setup


@pytest.mark.parametrize(
    ("resource", "expected_tables", "configs"),
    (
        ("sleep", 1, {"max_table_nesting": 2}),
        ("steps", 1, {"max_table_nesting": 1}),
        ("exercise", 1, {"max_table_nesting": 1}),
    ),
)
class TestGoogleHealthPhases:

    def test_extract(
        self,
        mock_google_health_apis,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):

        # GIVEN
        # Mocked APIs
        mock_google_health_apis(endpoints=[resource])

        # WHEN
        source = google_health_source(access_token="dummy_token").with_resources(
            f"google_health__{resource}",
        )
        info = duckdb_pipeline.extract(source)

        # THEN
        assert len(info.loads_ids) == 1

    def test_normalize(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):

        # GIVEN
        expected_rows = 3
        file_name = f"google_health_{resource}_run1-page1.json"
        source = sample_resource(
            file_name,
            resource_configs=configs,
            data_selector="dataPoints",
        )
        duckdb_pipeline.extract(source, table_name=resource)

        # WHEN
        info = duckdb_pipeline.normalize()

        # THEN
        # _dlt_state table, source table, and any child tables
        assert (
            len([r for r in info.row_counts if r.startswith(resource)])
            == expected_tables
        )
        # first page record count
        assert info.row_counts[resource] == expected_rows

    def test_load(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):
        # GIVEN
        # Files to load for sample test
        file_name = f"google_health_{resource}_run1-page1.json"
        source = sample_resource(
            file_name,
            resource_configs=configs,
            data_selector="dataPoints",
        )
        duckdb_pipeline.extract(source, table_name=f"load_{resource}")
        duckdb_pipeline.normalize()

        # WHEN
        info = duckdb_pipeline.load()

        # THEN
        assert info.has_failed_jobs is False
        assert all(p.state == "loaded" for p in info.load_packages)


@pytest.mark.parametrize(
    ("resource", "increment"),
    (
        ("sleep", True),
        ("steps", True),
        ("exercise", True),
        ("sleep", False),
        ("steps", False),
        ("exercise", False),
    ),
)
def test_google_health_refresh(
    mock_google_health_apis,
    duckdb_pipeline: dlt.Pipeline,
    resource: str,
    increment: bool,
):
    # GIVEN (1)
    # Mock APIs
    mock_google_health_apis(endpoints=[resource])
    expected_rows = 5  # 3 from page 1 + 2 from page 2
    # Dataset from pipeline (dev mode)
    dataset = duckdb_pipeline.dataset_name
    table = resource
    # Defaults to append for most resources
    write_disposition = None if increment else "replace"

    # WHEN (1)
    source = google_health_source(access_token="dummy_token").with_resources(f"google_health__{resource}")
    info = duckdb_pipeline.run(source)

    # THEN (1)
    # Run pipeline the first time
    assert info.first_run is True
    assert info.has_failed_jobs is False
    # Validate loaded data from initial run
    with duckdb_pipeline.sql_client() as client:
        table_rows = client.execute_sql(f"SELECT 1 FROM {dataset}.google_health__{table}")
    assert len(table_rows) == expected_rows

    # GIVEN (2)
    expected_rows += 1 if increment else 0

    # WHEN (2)
    info2 = duckdb_pipeline.run(source, write_disposition=write_disposition)

    # THEN (2)
    # Run pipeline again
    assert info2.first_run is False
    assert info2.has_failed_jobs is False
    # Validate loaded data from incremental run
    with duckdb_pipeline.sql_client() as client:
        table_rows2 = client.execute_sql(f"SELECT 1 FROM {dataset}.google_health__{table}")
    assert len(table_rows2) == expected_rows


def test_google_health_pipeline(mock_google_health_apis, duckdb_pipeline):
    """
    Test that the Google Health pipeline runs and loads data correctly.
    """
    # GIVEN
    mock_google_health_apis()

    # WHEN
    # Run the pipeline
    source = google_health_source(access_token="dummy_token")
    info = duckdb_pipeline.run(source)

    # THEN
    # Assert that the pipeline ran successfully
    assert info.first_run is True
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    # Check the loaded data
    dataset = duckdb_pipeline.dataset_name
    with duckdb_pipeline.sql_client() as client:
        # Check sleep table — use DISTINCT name to guard against dlt loading
        # records more than once when multiple resources run together (dlt>=1.23)
        sleep_table = client.execute_sql(
            f"SELECT DISTINCT name FROM {dataset}.google_health__sleep"
        )
        assert len(sleep_table) == 5

        steps_table = client.execute_sql(
            f"SELECT DISTINCT name FROM {dataset}.google_health__steps"
        )
        assert len(steps_table) == 5

        exercise_table = client.execute_sql(
            f"SELECT DISTINCT name FROM {dataset}.google_health__exercise"
        )
        assert len(exercise_table) == 5
