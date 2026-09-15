"""Live integration tests for the Google Health pipeline.

Calls the real Google Health API and loads into local DuckDB. Opt-in:
skipped unless ``RUN_LIVE_API_TESTS=1`` is set.
"""

import os

import dlt
import pytest

from tests.test_live_test_range import (
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


class TestGoogleHealthIntegration:
    """Live Google Health API → DuckDB checks over a 3-week window with small pages."""

    def test_google_health_refresh(self, duckdb_pipeline: dlt.Pipeline):
        """Refresh all Google Health resources against the live API."""
        from pipelines.runner import refresh_pipeline

        info = refresh_pipeline(
            "google_health",
            **LIVE_REFRESH_ARGS,
            pipeline=duckdb_pipeline,
            source_modifier=live_source_modifier("google_health"),
        )

        assert info.has_failed_jobs is False

        log_live_row_counts(
            duckdb_pipeline,
            [
                "google_health__sleep",
                "google_health__steps",
                "google_health__exercise",
            ],
        )
