"""Unit tests for the mock dbt seed exporter.

Covers ``scripts.fixtures.export_mock_seeds`` (source discovery, BigQuery
sampling/normalization, parallel export orchestration) without touching real
GCP credentials or the network — the BigQuery client is stubbed.
"""

from __future__ import annotations

import csv
import datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from scripts.fixtures import export_mock_seeds
from scripts.fixtures.scrub_data import (
    apply_fakes_to_rows,
    resolve_project_root,
    scrub_api_response,
    seed_faker,
)


@pytest.fixture(autouse=True)
def _export_env(monkeypatch):
    """Provide the exporter env config for every test."""
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("DBT_RAW_DATASET", "raw_test")
    monkeypatch.delenv("MOCK_SEED_SAMPLE_ROWS", raising=False)
    monkeypatch.delenv("MOCK_SEED_MAX_WORKERS", raising=False)
    seed_faker(42)


class _FakeQueryJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return iter(self._rows)


class _FakeClient:
    """Stub BigQuery client returning canned rows per table identifier."""

    def __init__(self, rows_by_identifier=None, fail_on=()):
        self.rows_by_identifier = rows_by_identifier or {}
        self.fail_on = set(fail_on)
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        identifier = sql.rsplit(".", 1)[-1].split("`")[0]
        if identifier in self.fail_on:
            raise RuntimeError(f"boom: {identifier}")
        return _FakeQueryJob(self._rows_for(identifier))

    def _rows_for(self, identifier):
        rows = self.rows_by_identifier.get(identifier, [])
        return [dict(row) for row in rows]


def _sample_rows():
    rows = [
        {
            "id": 1,
            "properties__email": "real.person@example.com",
            "properties__hs_note_body": "caller discussed renewal terms",
            "created": datetime.datetime(2024, 3, 10, 12, 30, 0, tzinfo=datetime.timezone.utc),
            "day": datetime.date(2024, 3, 10),
            "amount": Decimal("19.99"),
            "raw": b"bytes-value",
            "meta": {"source": "web", "score": 3},
            "tags": ["a", "b"],
            "payload": '{"a": 1}',
        }
    ]
    rows.extend(
        {
            "id": i,
            "properties__email": f"user{i}@example.com",
            "properties__hs_note_body": f"note body {i}",
            "created": datetime.datetime(2024, 3, 10 + i, 12, 30, 0, tzinfo=datetime.timezone.utc),
            "day": datetime.date(2024, 3, 10 + i),
            "amount": Decimal(f"{i}.99"),
            "raw": f"bytes-{i}".encode(),
            "meta": {"source": "web", "score": i},
            "tags": ["a", "b"],
            "payload": '{"a": 1}',
        }
        for i in range(2, 6)
    )
    return rows


def _write_sources_yaml(directory: Path, filename: str, payload: dict) -> Path:
    path = directory / filename
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# get_config
# ---------------------------------------------------------------------------


def test_get_config_reads_environment(monkeypatch):
    monkeypatch.setenv("MOCK_SEED_SAMPLE_ROWS", "10")
    monkeypatch.setenv("MOCK_SEED_MAX_WORKERS", "2")

    config = export_mock_seeds.get_config()

    assert config["project_id"] == "test-project"
    assert config["raw_dataset"] == "raw_test"
    assert config["sample_rows"] == 10
    assert config["max_workers"] == 2
    assert config["output_dir"].name == "test_fixtures"


def test_get_config_requires_project_and_dataset(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)

    with pytest.raises(RuntimeError, match="GCP_PROJECT_ID"):
        export_mock_seeds.get_config()


def test_get_config_falls_back_on_invalid_ints(monkeypatch):
    monkeypatch.setenv("MOCK_SEED_SAMPLE_ROWS", "not-a-number")
    monkeypatch.setenv("MOCK_SEED_MAX_WORKERS", "0")

    config = export_mock_seeds.get_config()

    assert config["sample_rows"] == export_mock_seeds.DEFAULT_SAMPLE_ROWS
    assert config["max_workers"] == 1


# ---------------------------------------------------------------------------
# load_source_definitions
# ---------------------------------------------------------------------------


def test_load_source_definitions_discovers_tables_dynamically(tmp_path):
    staging = tmp_path / "staging" / "hubspot"
    staging.mkdir(parents=True)
    _write_sources_yaml(
        staging,
        "hubspot_sources.yml",
        {
            "sources": [
                {
                    "name": "hubspot",
                    "tables": [
                        {"name": "contacts"},
                        {"name": "assoc", "identifier": "hubspot__contacts_to_deals"},
                    ],
                }
            ]
        },
    )
    _write_sources_yaml(tmp_path / "staging", "empty_sources.yml", {"sources": []})
    _write_sources_yaml(staging, "broken_sources.yml", {"sources": [{"tables": [{"name": "x"}]}]})

    defs = export_mock_seeds.load_source_definitions(staging_dir=tmp_path / "staging")

    assert ("hubspot", "contacts", "hubspot__contacts") in defs
    assert ("hubspot", "assoc", "hubspot__contacts_to_deals") in defs
    assert all(source and table and identifier for source, table, identifier in defs)


# ---------------------------------------------------------------------------
# normalize_row_values / json_serial
# ---------------------------------------------------------------------------


def test_normalize_row_values_converts_bigquery_types():
    row = {
        "when": datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        "day": datetime.date(2024, 1, 2),
        "amount": Decimal("3.5"),
        "raw": b"abc",
        "meta": {"a": 1},
        "tags": [1, 2],
        "payload": '{"a": 1}',
        "plain": "hello",
        "count": 7,
    }

    normalized = export_mock_seeds.normalize_row_values(row)

    assert normalized["when"] == "2024-01-02T03:04:05+00:00"
    assert normalized["day"] == "2024-01-02"
    assert normalized["amount"] == 3.5
    assert normalized["raw"] == "abc"
    assert normalized["meta"] == '{"a": 1}'
    assert normalized["tags"] == "[1, 2]"
    assert normalized["payload"] == '{"a": 1}'
    assert normalized["plain"] == "hello"
    assert normalized["count"] == 7


def test_json_serial_rejects_unknown_types():
    with pytest.raises(TypeError, match="not serializable"):
        export_mock_seeds.json_serial(object())


# ---------------------------------------------------------------------------
# export_table
# ---------------------------------------------------------------------------


def test_export_table_samples_scrubs_and_writes_csv(tmp_path):
    client = _FakeClient({"hubspot__contacts": _sample_rows()})
    out_dir = tmp_path / "mock_sources"

    result = export_mock_seeds.export_table(
        client,
        "hubspot",
        "hubspot__contacts",
        project_id="test-project",
        raw_dataset="raw_test",
        sample_rows=5,
        output_dir=out_dir,
    )

    assert result == out_dir / "hubspot" / "hubspot__contacts.csv"
    assert "LIMIT 5" in client.queries[0]
    assert "raw_test.hubspot__contacts" in client.queries[0]
    with open(result, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 5
    # PII scrubbed, rich types normalized to CSV-safe strings.
    assert rows[0]["properties__email"] != "real.person@example.com"
    assert "@" in rows[0]["properties__email"]
    assert rows[0]["properties__hs_note_body"] != "caller discussed renewal terms"
    assert rows[0]["created"] == "2024-03-10T12:30:00+00:00"
    assert rows[0]["amount"] == "19.99"
    assert rows[0]["meta"] == '{"source": "web", "score": 3}'


def test_export_table_returns_none_when_table_is_empty(tmp_path):
    client = _FakeClient({"hubspot__contacts": []})

    result = export_mock_seeds.export_table(
        client,
        "hubspot",
        "hubspot__contacts",
        project_id="test-project",
        raw_dataset="raw_test",
        output_dir=tmp_path,
    )

    assert result is None
    assert list(tmp_path.rglob("*.csv")) == []


def test_export_table_dry_run_writes_nothing(tmp_path):
    client = _FakeClient({"hubspot__contacts": _sample_rows()})

    result = export_mock_seeds.export_table(
        client,
        "hubspot",
        "hubspot__contacts",
        project_id="test-project",
        raw_dataset="raw_test",
        output_dir=tmp_path,
        dry_run=True,
    )

    assert result is not None
    assert list(tmp_path.rglob("*.csv")) == []


# ---------------------------------------------------------------------------
# export_all
# ---------------------------------------------------------------------------


def test_export_all_runs_tables_in_parallel_and_summarizes(tmp_path):
    defs = [
        ("hubspot", "contacts", "hubspot__contacts"),
        ("hubspot", "empty", "hubspot__empty"),
        ("notion", "broken", "notion__broken"),
    ]
    client = _FakeClient(
        {
            "hubspot__contacts": _sample_rows(),
            "hubspot__empty": [],
        },
        fail_on={"notion__broken"},
    )

    summary = export_mock_seeds.export_all(
        client,
        defs,
        project_id="test-project",
        raw_dataset="raw_test",
        output_dir=tmp_path,
        max_workers=2,
    )

    assert summary["exported"] == ["hubspot__contacts"]
    assert summary["skipped"] == ["hubspot__empty"]
    assert summary["failed"] == ["notion__broken"]
    assert (tmp_path / "hubspot" / "hubspot__contacts.csv").exists()


def test_export_all_supports_source_filter_and_empty_definitions(tmp_path):
    defs = [
        ("hubspot", "contacts", "hubspot__contacts"),
        ("notion", "pages", "notion__pages"),
    ]
    client = _FakeClient({"hubspot__contacts": _sample_rows(), "notion__pages": _sample_rows()})

    summary = export_mock_seeds.export_all(
        client,
        defs,
        project_id="test-project",
        raw_dataset="raw_test",
        output_dir=tmp_path,
        source_filter="notion",
    )

    assert summary["exported"] == ["notion__pages"]
    assert len(client.queries) == 1

    assert export_mock_seeds.export_all(
        client,
        [],
        project_id="test-project",
        raw_dataset="raw_test",
        output_dir=tmp_path,
    ) == {"exported": [], "failed": [], "skipped": []}


# ---------------------------------------------------------------------------
# main / CLI
# ---------------------------------------------------------------------------


def test_build_parser_accepts_source_and_tuning_flags():
    args = export_mock_seeds.build_parser().parse_args(
        ["--source", "hubspot", "--sample-rows", "7", "--max-workers", "3", "--dry-run"]
    )

    assert args.source == "hubspot"
    assert args.sample_rows == 7
    assert args.max_workers == 3
    assert args.dry_run is True


def test_main_wires_config_definitions_and_client(monkeypatch, tmp_path):
    import google.cloud.bigquery as bigquery_module

    defs = [("hubspot", "contacts", "hubspot__contacts")]
    client = _FakeClient({"hubspot__contacts": _sample_rows()})
    monkeypatch.setattr(export_mock_seeds, "load_source_definitions", lambda staging_dir=None: defs)
    monkeypatch.setattr(bigquery_module, "Client", lambda project=None: client)

    summary = export_mock_seeds.main(["--source", "hubspot", "--sample-rows", "5", "--dry-run"])

    assert summary == {"exported": ["hubspot__contacts"], "failed": [], "skipped": []}
    assert "LIMIT 5" in client.queries[0]


# ---------------------------------------------------------------------------
# scrub_data dynamic matching + project root resolution
# ---------------------------------------------------------------------------


def test_apply_fakes_to_rows_returns_rows_and_matches_suffixes():
    seed_faker(7)
    rows = [
        {
            "id": 5,
            "properties__email": "someone@example.com",
            "custom_meeting_notes": "discussed pricing",
            "count": 3,
        }
    ]

    result = apply_fakes_to_rows(rows)

    assert result is rows
    assert rows[0]["id"] == 5
    assert rows[0]["count"] == 3
    assert rows[0]["properties__email"] != "someone@example.com"
    assert rows[0]["custom_meeting_notes"] != "discussed pricing"


def test_scrub_api_response_detects_dynamic_pii_keys():
    seed_faker(11)
    payload = {
        "order_title": "my secret deal",
        "contact_email_address": "boss@example.com",
        "contact.name": "Jane Doe",
        "id": 99,
        "nested": {"internal_notes": "call back tomorrow"},
    }

    scrubbed = scrub_api_response(payload)

    assert scrubbed["order_title"] != "my secret deal"
    assert scrubbed["contact_email_address"] != "boss@example.com"
    assert scrubbed["contact.name"] != "Jane Doe"
    assert scrubbed["id"] == 99
    assert scrubbed["nested"]["internal_notes"] != "call back tomorrow"


def test_resolve_project_root_finds_repo_markers(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)

    assert resolve_project_root(nested) == tmp_path
