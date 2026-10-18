"""Configuration and fixtures for DLT unit tests.

This module provides common fixtures, helper functions, and configuration
for testing DLT (Data Load Tool) pipeline components.
"""

# base imports
import json
import os
from pathlib import Path
from typing import Any

import dlt

# PyPI imports
import pytest

MOCK_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
MOCK_FOLDER = str(MOCK_ROOT)


def resolve_mock_path(file_name: str) -> str:
    """Resolve fixture paths under the system-scoped tests/fixtures layout."""
    if os.path.isabs(file_name):
        return file_name

    path = Path(file_name)
    candidate = (MOCK_ROOT / path).resolve()
    if candidate.exists():
        return str(candidate)

    system_prefix = file_name.split("__", 1)[0] if "__" in file_name else file_name.split("_", 1)[0]
    candidate = (MOCK_ROOT / system_prefix / path.name).resolve()
    if candidate.exists():
        return str(candidate)

    return str((MOCK_ROOT / path).resolve())


# This environment variable is set to disable the Google Secrets provider for all dlt unit tests.
# It needs to be set before any dlt modules are imported, which is why it's at the top of this file.
# Only applies to unit tests - E2E tests should not be affected
os.environ["PROVIDERS__ENABLE_GOOGLE_SECRETS"] = "false"
os.environ["DLT_TELEMETRY_DISABLED"] = "1"


@pytest.fixture
def mock_responses():
    """Pytest fixture providing a requests mock for HTTP calls.

    Yields:
        responses.RequestsMock: Mock object for intercepting HTTP requests.
    """
    import responses

    with responses.RequestsMock() as rsps:
        yield rsps


def sample_resource(
    file_name: str,
    fallback: str | None = None,
    data_selector: str | None = None,
    resource_configs: dict | None = None,
) -> Any:
    """Create a DLT resource from sample JSON data for testing.

    Args:
        file_name: Name of the JSON file in the mock data folder.
        fallback: Optional fallback file name if the primary file doesn't exist.
        data_selector: Optional JSON key to select specific data from the file.
        resource_configs: Additional DLT resource configuration options.

    Returns:
        DLT resource configured with the sample data.
    """

    @dlt.resource(**(resource_configs or {}))
    def sample_resource():
        source = sample_data(file_name=file_name, fallback=fallback)
        if data_selector:
            source = source[data_selector]
        return source

    return sample_resource


def sample_data(
    file_name: str,
    fallback: str | None = None,
) -> dict:
    """Load sample JSON data from the mock data folder.

    Args:
        file_name: Name of the JSON file to load.
        fallback: Optional fallback file name if the primary file doesn't exist.

    Returns:
        dict: Parsed JSON data from the file.
    """
    file = resolve_mock_path(file_name)
    if not os.path.exists(file) and fallback is not None:
        file = resolve_mock_path(fallback)
    with open(file, "r") as f:
        return json.load(f)


def sample_response(file_name: str) -> tuple[int, dict, str]:
    """Create a mock HTTP response from a JSON file.

    Args:
        file_name: Name of the JSON file to use as response body.

    Returns:
        tuple: HTTP response tuple (status_code, headers, body).
    """
    with open(resolve_mock_path(file_name), "r") as f:
        return (200, {}, f.read())


def pipeline_state(pipeline: dlt.Pipeline) -> dict[str, Any]:
    """Read the persisted dlt state for incremental assertions."""
    state_path = Path(pipeline._pipeline_storage.storage_path) / "state.json"
    return json.loads(state_path.read_text())


def incremental_last_value(pipeline: dlt.Pipeline, resource_name: str, cursor_path: str) -> Any:
    """Find a resource cursor without depending on dlt's source function name."""
    state = pipeline_state(pipeline)
    for source in state.get("sources", {}).values():
        resource = source.get("resources", {}).get(resource_name)
        if resource:
            incremental = resource["incremental"]
            if cursor_path in incremental:
                return incremental[cursor_path]["last_value"]
            for configured_path, values in incremental.items():
                if configured_path.endswith(f".{cursor_path}"):
                    return values["last_value"]
    raise AssertionError(f"No persisted state found for {resource_name}")


@pytest.fixture(scope="class")
def duckdb_pipeline(tmp_path_factory: pytest.TempPathFactory) -> dlt.Pipeline:
    """Pytest fixture providing a DuckDB pipeline for unit testing.

    Creates a DLT pipeline configured to use DuckDB as the destination
    for fast local testing. The pipeline is automatically cleaned up
    after each test class.

    Yields:
        dlt.Pipeline: Configured DLT pipeline for testing.
    """
    pipelines_dir = tmp_path_factory.mktemp("dlt_pipelines")
    database_path = pipelines_dir / "local_unit_test.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="local_unit_test",
        destination=dlt.destinations.duckdb(credentials=str(database_path)),
        dataset_name="local_data",
        dev_mode=True,
        pipelines_dir=str(pipelines_dir),
    )
    yield pipeline
    try:
        pipeline.drop()
    except Exception:
        pass
