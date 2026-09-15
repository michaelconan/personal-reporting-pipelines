"""Live integration tests for the Notion pipeline.

Calls the real Notion API and loads into local DuckDB. Opt-in:
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


class TestNotionIntegration:
    """Live Notion API → DuckDB checks over a 3-week window with small pages."""

    def test_notion_refresh(self, duckdb_pipeline: dlt.Pipeline):
        """Refresh all Notion resources against the live API."""
        from pipelines.runner import refresh_pipeline

        info = refresh_pipeline(
            "notion",
            **LIVE_REFRESH_ARGS,
            pipeline=duckdb_pipeline,
            source_modifier=live_source_modifier("notion"),
        )

        assert info.has_failed_jobs is False

        # Data-source search has no date filter, so it must return rows
        # even when the narrowed row window is empty.
        dataset = duckdb_pipeline.dataset_name
        with duckdb_pipeline.sql_client() as client:
            sources = client.execute_sql(f"SELECT 1 FROM {dataset}.notion__data_sources")
            assert len(sources) >= 1
        log_live_row_counts(
            duckdb_pipeline,
            [
                "notion__data_source_daily_habits",
                "notion__data_source_weekly_habits",
                "notion__data_source_monthly_habits",
            ],
        )
