"""Tests for dbt source freshness configuration.

Source freshness measures data age from dlt's load metadata: each raw row's
``_dlt_load_id`` is joined to dlt's ``_dlt_loads`` table and the latest
completed load's ``inserted_at`` is compared against the SLA thresholds.

The ``loaded_at_query`` must reference ``_dlt_loads`` as a per-part quoted
multipart identifier. On Databricks a single backtick span that contains dots
(`` `workspace.raw._dlt_loads` ``) is parsed as ONE identifier rather than a
catalog.schema.table path, so the lookup fails with TABLE_OR_VIEW_NOT_FOUND.
"""

from __future__ import annotations

import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml

DBT_MODELS = Path(__file__).resolve().parents[3] / "dbt" / "models"

SOURCES: list[tuple[str, str]] = [
    ("hubspot", "staging/hubspot/_hubspot__sources.yml"),
    ("notion", "staging/notion/_notion__sources.yml"),
    ("google_health", "staging/google_health/_google_health__sources.yml"),
]

# (warn_after_hours, error_after_hours) per ingestion cadence
EXPECTED_SLA: dict[str, tuple[int, int]] = {
    "hubspot": (48, 72),  # daily schedule
    "notion": (336, 384),  # weekly (14 / 16 days)
    "google_health": (336, 384),  # weekly
}


def _source(name: str, rel: str) -> dict[str, Any]:
    data = yaml.safe_load((DBT_MODELS / rel).read_text())
    return data["sources"][0]


def _hours(block: dict[str, Any]) -> int:
    return int(block["count"])


@pytest.mark.parametrize(("name", "rel"), SOURCES)
def test_source_defines_freshness_sla(name: str, rel: str) -> None:
    freshness = _source(name, rel)["config"]["freshness"]
    assert "warn_after" in freshness
    assert "error_after" in freshness


@pytest.mark.parametrize(("name", "rel"), SOURCES)
def test_sla_thresholds_match_ingestion_cadence(name: str, rel: str) -> None:
    freshness = _source(name, rel)["config"]["freshness"]
    warn, error = _hours(freshness["warn_after"]), _hours(freshness["error_after"])
    assert (warn, error) == EXPECTED_SLA[name]


@pytest.mark.parametrize(("name", "rel"), SOURCES)
def test_loaded_at_query_references_dlt_loads(name: str, rel: str) -> None:
    query = _source(name, rel)["config"]["loaded_at_query"]
    assert "_dlt_loads" in query


@pytest.mark.parametrize(("name", "rel"), SOURCES)
def test_loaded_at_query_joins_on_dlt_load_id(name: str, rel: str) -> None:
    query = _source(name, rel)["config"]["loaded_at_query"]
    assert "_dlt_load_id" in query
    assert "load_id" in query


@pytest.mark.parametrize(("name", "rel"), SOURCES)
def test_loaded_at_query_filters_completed_loads(name: str, rel: str) -> None:
    query = _source(name, rel)["config"]["loaded_at_query"]
    assert re.search(r"status\s*=\s*0", query)


@pytest.mark.parametrize(("name", "rel"), SOURCES)
def test_loaded_at_query_aggregates_max_inserted_at(name: str, rel: str) -> None:
    query = _source(name, rel)["config"]["loaded_at_query"]
    assert "max(" in query.lower()
    assert "inserted_at" in query


@pytest.mark.parametrize(("name", "rel"), SOURCES)
def test_loaded_at_query_does_not_coalesce(name: str, rel: str) -> None:
    query = _source(name, rel)["config"]["loaded_at_query"].lower()
    assert "coalesce" not in query
    assert "current_timestamp" not in query


@pytest.mark.parametrize(("name", "rel"), SOURCES)
def test_dlt_loads_referenced_with_per_part_quoting(name: str, rel: str) -> None:
    query = _source(name, rel)["config"]["loaded_at_query"]
    assert "`{{ this.database }}.{{ this.schema }}._dlt_loads`" not in query, (
        f"{name}: `_dlt_loads` is referenced inside a single backtick span that "
        "contains dots. Databricks parses it as one identifier, so the table "
        "resolves to nothing (TABLE_OR_VIEW_NOT_FOUND). Quote each part "
        "separately: `catalog`.`schema`.`table`."
    )


def _render_for_duckdb(query: str) -> str:
    """Substitute dbt Jinja and adapt the Databricks identifier for DuckDB."""
    query = query.replace(
        "`{{ this.database }}`.`{{ this.schema }}`.`_dlt_loads`", "raw._dlt_loads"
    )
    query = query.replace("{{ this }}", "raw_contacts")
    return query


def test_loaded_at_query_executes_against_duckdb() -> None:
    query = _render_for_duckdb(_source("hubspot", SOURCES[0][1])["config"]["loaded_at_query"])

    with tempfile.TemporaryDirectory() as tmp:
        con = duckdb.connect(str(Path(tmp) / "freshness.duckdb"))
        con.execute("CREATE SCHEMA raw")
        con.execute(
            "CREATE OR REPLACE TABLE raw._dlt_loads "
            "(load_id VARCHAR, status INTEGER, inserted_at TIMESTAMP)"
        )
        con.execute("CREATE OR REPLACE TABLE raw_contacts (id INTEGER, _dlt_load_id VARCHAR)")
        con.execute(
            "INSERT INTO raw._dlt_loads VALUES "
            "('ok', 0, TIMESTAMP '2026-10-18 02:00:00'), "
            "('failed', 1, TIMESTAMP '2026-10-19 02:00:00')"
        )
        con.execute("INSERT INTO raw_contacts VALUES (1, 'ok'), (2, 'failed')")

        result = con.execute(query).fetchone()[0]
        assert result == datetime(2026, 10, 18, 2, 0, 0)

        con.execute("UPDATE raw._dlt_loads SET status = 1 WHERE load_id = 'ok'")
        assert con.execute(query).fetchone()[0] is None
