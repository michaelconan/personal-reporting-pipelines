"""Live integration tests for the HubSpot pipeline.

Calls the real HubSpot API and loads into local DuckDB. Opt-in:
skipped unless ``RUN_LIVE_API_TESTS=1`` is set.
"""

import os

import dlt
import pytest

from tests.live_test_range import (
    LIVE_REFRESH_ARGS,
    live_source_modifier,
    log_live_row_counts,
)

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.getenv("RUN_LIVE_API_TESTS") != "1",
        reason="Set RUN_LIVE_API_TESTS=1 to call live APIs",
    ),
]


class TestHubSpotIntegration:
    """Live HubSpot API → DuckDB checks over a 3-week window with small pages."""

    def test_hubspot_refresh(self, duckdb_pipeline: dlt.Pipeline):
        """Refresh all HubSpot resources against the live API."""
        from pipelines.runner import refresh_pipeline

        info = refresh_pipeline(
            "hubspot",
            **LIVE_REFRESH_ARGS,
            pipeline=duckdb_pipeline,
            source_modifier=live_source_modifier("hubspot"),
        )

        assert info.has_failed_jobs is False

        # Schema resources are unfiltered, so they must load rows even
        # when the narrowed object window is empty.
        dataset = duckdb_pipeline.dataset_name
        with duckdb_pipeline.sql_client() as client:
            schemas = client.execute_sql(f"SELECT 1 FROM {dataset}.hubspot__schemas")
            assert len(schemas) >= 1
        log_live_row_counts(
            duckdb_pipeline,
            [
                "hubspot__contacts",
                "hubspot__companies",
                "hubspot__meetings",
                "hubspot__calls",
                "hubspot__notes",
            ],
        )
