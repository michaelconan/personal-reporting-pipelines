"""Shared constants for live API tests (integration + e2e).

Live tests call real external APIs over a 3-week window with small page
sizes (applied via the mock-export script's paginator override helper),
so pagination is exercised while runtime and API quota stay bounded.
"""

from collections.abc import Callable
from typing import Any, TypedDict

LIVE_INITIAL_DATE = "2025-06-01"
LIVE_END_DATE = "2025-06-22"


class LiveRefreshArgs(TypedDict):
    is_incremental: bool
    initial_date: str
    end_date: str


LIVE_REFRESH_ARGS: LiveRefreshArgs = {
    "is_incremental": False,
    "initial_date": LIVE_INITIAL_DATE,
    "end_date": LIVE_END_DATE,
}

# Page-size forcing per pipeline: (request property, default value,
# per-resource values). Small pages force multi-page extraction so live
# tests exercise pagination.
# NOTE: google_health exercise rejects its own next-page token when
# pageSize=5 (400 INVALID_PAGE_TOKEN), so sleep/exercise use 10; steps
# keeps the source default (1000) because 3,542 rows/page-10 would mean
# ~355 requests — it still paginates (4 pages) at the default.
LIVE_PAGE_OVERRIDES = {
    "notion": ("page_size", 5, None),
    # limit=2: the 3-week window is quiet (4 meetings, no contact/company
    # updates), so only a tiny limit forces multi-page extraction.
    "hubspot": ("limit", 2, None),
    "google_health": ("pageSize", 10, {"google_health__steps": 1000}),
}


def live_source_modifier(pipeline_key: str) -> Callable[[Any], Any]:
    """Build a ``source_modifier`` forcing small pages for a live test run.

    Reuses ``override_incremental_resource_property`` from the mock-export
    script, so only incremental (paginated) resources are affected.
    """

    def _modify(source: Any) -> Any:
        from scripts.fixtures.export_mock_responses import (
            override_incremental_resource_property,
        )

        prop, value, resource_values = LIVE_PAGE_OVERRIDES[pipeline_key]
        override_incremental_resource_property(
            source, prop, value=value, resource_values=resource_values
        )
        return source

    return _modify


def log_live_row_counts(pipeline: Any, tables: list) -> None:
    """Print per-table row counts; tolerates tables absent on empty windows.

    dlt creates no table for a resource that returns zero rows, so a
    missing table is an expected outcome, not a failure.
    """
    dataset = pipeline.dataset_name
    with pipeline.sql_client() as client:
        for table in tables:
            try:
                rows = client.execute_sql(f"SELECT COUNT(*) FROM {dataset}.{table}")
                print(f"LIVE ROWS {table}: {rows[0][0]}")
            except Exception as exc:
                print(f"LIVE ROWS {table}: missing ({exc})")
