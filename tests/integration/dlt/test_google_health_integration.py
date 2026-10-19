"""Live integration tests for the Google Health pipeline.

Calls the real Google Health API and loads into local DuckDB. Opt-in:
skipped unless ``RUN_LIVE_API_TESTS=1`` is set.
"""

import os
from datetime import date, timedelta
from urllib.parse import parse_qs, urlparse

import dlt
import pytest
import requests

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

    def test_google_health_refresh(
        self, duckdb_pipeline: dlt.Pipeline, monkeypatch: pytest.MonkeyPatch
    ):
        """Refresh all Google Health resources against the live API."""
        from pipelines.runner import refresh_pipeline

        urls: list[str] = []
        original_send = requests.Session.send

        def _recording_send(session, request, **kwargs):  # noqa: ANN001, ANN003
            urls.append(request.url)
            return original_send(session, request, **kwargs)

        monkeypatch.setattr(requests.Session, "send", _recording_send)

        info = refresh_pipeline(
            "google_health",
            **LIVE_REFRESH_ARGS,
            pipeline=duckdb_pipeline,
            source_modifier=live_source_modifier("google_health"),
        )

        assert info.has_failed_jobs is False

        # Pin the exercise filter: the UTC window is widened one day on each
        # side and rendered as an offset-less civil date.
        exercise_url = next(url for url in urls if "dataTypes/exercise/dataPoints" in url)
        (exercise_filter,) = parse_qs(urlparse(exercise_url).query)["filter"]
        initial = date.fromisoformat(LIVE_REFRESH_ARGS["initial_date"])
        end = date.fromisoformat(LIVE_REFRESH_ARGS["end_date"])
        lower = (initial - timedelta(days=1)).isoformat()
        upper = (end + timedelta(days=1)).isoformat()
        assert exercise_filter == (
            f'exercise.interval.civil_start_time >= "{lower}"'
            f' AND exercise.interval.civil_start_time < "{upper}"'
        )

        log_live_row_counts(
            duckdb_pipeline,
            [
                "google_health__sleep",
                "google_health__steps",
                "google_health__exercise",
            ],
        )
