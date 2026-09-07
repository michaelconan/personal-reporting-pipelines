"""Configuration and fixtures for DLT live integration tests.

Integration tests call real external APIs but load into local DuckDB
(instead of BigQuery like the e2e suite) so they can run without GCP.
They are opt-in: skipped unless ``RUN_LIVE_API_TESTS=1`` is set.
"""

import os
from collections.abc import Generator

import dlt
import pytest

os.environ["DLT_TELEMETRY_DISABLED"] = "1"


@pytest.fixture(scope="class")
def duckdb_pipeline(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[dlt.Pipeline, None, None]:
    """Pytest fixture providing an isolated DuckDB pipeline for live tests.

    Pipeline working state goes to a pytest-managed temp dir via
    ``pipelines_dir``, so local dlt state never touches ``~/.dlt``.
    The pipeline is dropped after each test class.
    """
    pipelines_dir = tmp_path_factory.mktemp("dlt_pipelines")
    pipeline = dlt.pipeline(
        pipeline_name="live_integration_test",
        destination="duckdb",
        dataset_name="live_data",
        dev_mode=True,
        pipelines_dir=str(pipelines_dir),
    )
    yield pipeline
    try:
        pipeline.drop()
    except Exception:
        pass
