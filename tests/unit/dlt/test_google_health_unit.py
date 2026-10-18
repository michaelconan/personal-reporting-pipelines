# base imports
import json
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import dlt

# PyPI imports
import pytest
from pytest import MonkeyPatch

# local imports
from pipelines.sources.google_health import get_google_health_token, google_health_source
from tests.unit.dlt.conftest import (
    incremental_last_value,
    sample_data,
    sample_resource,
    sample_response,
)

pytestmark = pytest.mark.local


def test_google_health_token_uses_host_refreshed_access_token(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__ACCESS_TOKEN", "host-refreshed-token")

    assert get_google_health_token() == "host-refreshed-token"


def test_google_health_refreshes_oauth_token_when_direct_token_is_also_set(
    monkeypatch: MonkeyPatch,
):
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__ACCESS_TOKEN", "possibly-stale-token")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__REFRESH_TOKEN", "refresh-token")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_ID", "client-id")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_SECRET", "client-secret")
    credentials = MagicMock(token="refreshed-token", refresh_token="refresh-token")
    monkeypatch.setattr("pipelines.sources.google_health.Credentials", lambda **_: credentials)
    monkeypatch.setattr("pipelines.sources.google_health.Request", lambda: object())

    assert get_google_health_token() == "refreshed-token"
    credentials.refresh.assert_called_once()


def test_google_health_token_raises_when_no_credentials_configured(monkeypatch: MonkeyPatch):
    monkeypatch.delenv("SOURCES__GOOGLE_HEALTH__ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("SOURCES__GOOGLE_HEALTH__REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("SOURCES__GOOGLE_HEALTH__CLIENT_ID", raising=False)
    monkeypatch.delenv("SOURCES__GOOGLE_HEALTH__CLIENT_SECRET", raising=False)

    with pytest.raises(KeyError, match="sources.google_health.refresh_token"):
        get_google_health_token()


def test_google_health_token_raises_when_client_id_missing(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__REFRESH_TOKEN", "refresh-token")
    monkeypatch.delenv("SOURCES__GOOGLE_HEALTH__CLIENT_ID", raising=False)
    monkeypatch.delenv("SOURCES__GOOGLE_HEALTH__CLIENT_SECRET", raising=False)
    monkeypatch.delenv("SOURCES__GOOGLE_HEALTH__ACCESS_TOKEN", raising=False)

    with pytest.raises(KeyError, match="sources.google_health.client_id"):
        get_google_health_token()


def test_google_health_token_raises_when_client_secret_missing(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__REFRESH_TOKEN", "refresh-token")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_ID", "client-id")
    monkeypatch.delenv("SOURCES__GOOGLE_HEALTH__CLIENT_SECRET", raising=False)
    monkeypatch.delenv("SOURCES__GOOGLE_HEALTH__ACCESS_TOKEN", raising=False)

    with pytest.raises(KeyError, match="sources.google_health.client_secret"):
        get_google_health_token()


def test_google_health_token_updates_refresh_token_in_secret_manager(
    monkeypatch: MonkeyPatch,
):
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__REFRESH_TOKEN", "old-refresh-token")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_ID", "client-id")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_SECRET", "client-secret")
    monkeypatch.setattr("pipelines.sources.google_health.SECRET_STORE", "google")
    credentials = MagicMock(token="new-access-token", refresh_token="new-refresh-token")
    monkeypatch.setattr("pipelines.sources.google_health.Credentials", lambda **_: credentials)
    monkeypatch.setattr("pipelines.sources.google_health.Request", lambda: object())

    mock_client = MagicMock()
    monkeypatch.setattr(
        "pipelines.sources.google_health.secretmanager_v1.SecretManagerServiceClient",
        lambda: mock_client,
    )
    monkeypatch.setattr(
        "pipelines.sources.google_health.google.auth.default", lambda: (None, "test-project")
    )

    assert get_google_health_token() == "new-access-token"
    mock_client.add_secret_version.assert_called_once()


def test_google_health_token_updates_refresh_token_in_1password(
    monkeypatch: MonkeyPatch,
):
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__REFRESH_TOKEN", "old-refresh-token")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_ID", "client-id")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_SECRET", "client-secret")
    monkeypatch.setattr("pipelines.sources.google_health.SECRET_STORE", "1password")
    credentials = MagicMock(token="new-access-token", refresh_token="new-refresh-token")
    monkeypatch.setattr("pipelines.sources.google_health.Credentials", lambda **_: credentials)
    monkeypatch.setattr("pipelines.sources.google_health.Request", lambda: object())

    mock_update = MagicMock()
    monkeypatch.setattr("pipelines.common.utils.update_onepassword_item", mock_update)

    assert get_google_health_token() == "new-access-token"
    mock_update.assert_called_once_with(
        item_name="google-health",
        vault="reporting",
        field_updates={"refresh_token": "new-refresh-token"},
    )


@pytest.fixture
def mock_google_health_apis(monkeypatch: MonkeyPatch, mock_responses) -> Callable:
    BASE_URL = "https://health.googleapis.com/v4"

    # Mock the token to prevent credential errors
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_ID", "dummy_client_id")
    monkeypatch.setenv("SOURCES__GOOGLE_HEALTH__CLIENT_SECRET", "dummy_secret")

    initial_filters: dict[str, str] = {}
    exercise_first_page_calls = 0
    incremental_mode = True

    def cursor_callback(request, resource: str):
        """Handle cursor-based pagination for Google Health APIs."""
        nonlocal exercise_first_page_calls
        parsed_url = urlparse(request.url)
        params = parse_qs(parsed_url.query)
        page_token = params.get("pageToken", [""])[0]
        filter_val = params.get("filter", [""])[0]
        initial_filter = initial_filters.setdefault(resource, filter_val)
        if page_token:
            page1 = json.loads(sample_response(f"google_health__{resource}-run1_page1.json")[2])
            assert page_token == page1["nextPageToken"]
            return sample_response(f"google_health__{resource}-run1_page2.json")
        if resource == "exercise" and not filter_val and incremental_mode:
            exercise_first_page_calls += 1
            if exercise_first_page_calls > 1:
                return sample_response(f"google_health__{resource}-run2.json")
        if filter_val != initial_filter:
            return sample_response(f"google_health__{resource}-run2.json")
        return sample_response(f"google_health__{resource}-run1_page1.json")

    def setup(endpoints=None, incremental=True):
        """Nested function to only register mock endpoints for tests.

        Args:
            endpoints (list, optional): Specific endpoints to register. Defaults to None (all).
            incremental: Whether the test run should return the next incremental page.
        """
        nonlocal incremental_mode
        incremental_mode = incremental
        if endpoints is None:
            endpoints = []
        # Mock the API responses
        if not endpoints or "sleep" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/users/me/dataTypes/sleep/dataPoints",
                callback=lambda r: cursor_callback(r, "sleep"),
                content_type="application/json",
            )
        if not endpoints or "steps" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/users/me/dataTypes/steps/dataPoints",
                callback=lambda r: cursor_callback(r, "steps"),
                content_type="application/json",
            )
        if not endpoints or "exercise" in endpoints:
            mock_responses.add_callback(
                mock_responses.GET,
                BASE_URL + "/users/me/dataTypes/exercise/dataPoints",
                callback=lambda r: cursor_callback(r, "exercise"),
                content_type="application/json",
            )

    return setup


@pytest.mark.parametrize(
    ("resource", "expected_tables", "configs"),
    (
        ("sleep", 2, {"max_table_nesting": 2}),
        ("steps", 1, {"max_table_nesting": 4}),
        ("exercise", 1, {"max_table_nesting": 2}),
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
        file_name = f"google_health__{resource}-run1_page1.json"
        source = sample_resource(
            file_name,
            resource_configs=configs,
            data_selector="dataPoints",
        )
        duckdb_pipeline.extract(source, table_name=resource)

        # WHEN
        info = duckdb_pipeline.normalize()

        # THEN
        assert len([r for r in info.row_counts if r.startswith(resource)]) == expected_tables
        assert info.row_counts[resource] == expected_rows

    def test_load(
        self,
        duckdb_pipeline: dlt.Pipeline,
        resource: str,
        expected_tables: int,
        configs: dict | None,
    ):
        # GIVEN
        file_name = f"google_health__{resource}-run1_page1.json"
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
    mock_google_health_apis(endpoints=[resource], incremental=increment)
    expected_rows = 5  # 3 from page 1 + 2 from page 2
    dataset = duckdb_pipeline.dataset_name
    table = resource
    write_disposition = None if increment else "replace"

    # WHEN (1)
    source = google_health_source(access_token="dummy_token").with_resources(
        f"google_health__{resource}"
    )
    info = duckdb_pipeline.run(source)

    # THEN (1)
    assert info.first_run is True
    assert info.has_failed_jobs is False
    cursor_path = {
        "sleep": ("sleep", "interval", "endTime"),
        "steps": ("steps", "interval", "startTime"),
        "exercise": ("exercise", "interval", "startTime"),
    }[resource]

    def cursor(record: dict) -> str:
        value: Any = record
        for key in cursor_path:
            value = value[key]
        return value

    expected_cursor = max(
        cursor(item)
        for page in ("run1_page1", "run1_page2")
        for item in sample_data(f"google_health__{resource}-{page}.json")["dataPoints"]
    )
    assert (
        incremental_last_value(duckdb_pipeline, f"google_health__{resource}", cursor_path[-1])
        == expected_cursor
    )
    with duckdb_pipeline.sql_client() as client:
        table_rows = client.execute_sql(f"SELECT 1 FROM {dataset}.google_health__{table}")
    assert len(table_rows) == expected_rows

    # GIVEN (2)
    expected_rows = expected_rows + 1 if increment else expected_rows

    # WHEN (2)
    info2 = duckdb_pipeline.run(source, write_disposition=write_disposition)

    # THEN (2)
    assert info2.first_run is False
    assert info2.has_failed_jobs is False
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
    source = google_health_source(access_token="dummy_token")
    info = duckdb_pipeline.run(source)

    # THEN
    assert info.first_run is True
    assert info.has_failed_jobs is False
    assert len(info.loads_ids) == 1

    dataset = duckdb_pipeline.dataset_name
    with duckdb_pipeline.sql_client() as client:
        sleep_table = client.execute_sql(f"SELECT COUNT(*) FROM {dataset}.google_health__sleep")
        assert sleep_table[0][0] >= 3
