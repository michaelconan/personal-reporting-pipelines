# scripts/fixtures/export_mock_seeds.py
"""
Utility script to export a sample of rows from each dbt source table
into the mock seed files used for local DuckDB testing.

Source tables are discovered dynamically from the ``*_sources.yml`` files
under ``dbt/models/staging`` (no hardcoded table list), sampled from the
BigQuery raw schema populated by dlt, scrubbed of PII, and written to
``dbt/test_fixtures/<source>/<source>__<table>.csv``.

Prerequisites
------------
- Google Cloud credentials with access to the BigQuery project containing the
  raw dataset.
- Environment variables:
    * ``GCP_PROJECT_ID`` – BigQuery project ID.
    * ``DBT_RAW_DATASET`` – Dataset name where the raw tables live (e.g.
      ``raw_reporting``).
    * ``MOCK_SEED_SAMPLE_ROWS`` (optional) – rows sampled per table (default: 5).
    * ``MOCK_SEED_MAX_WORKERS`` (optional) – parallel BigQuery exports
      (default: 4).

Usage
-----
```bash
python scripts/fixtures/export_mock_seeds.py
python scripts/fixtures/export_mock_seeds.py --source hubspot --sample-rows 10
python scripts/fixtures/export_mock_seeds.py --dry-run
```
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from scripts.fixtures.scrub_data import apply_fakes_to_rows, resolve_project_root
except ImportError:  # direct script execution: expose the repo root, then retry
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.fixtures.scrub_data import apply_fakes_to_rows, resolve_project_root

PROJECT_ROOT = resolve_project_root(Path(__file__).resolve().parent)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_SAMPLE_ROWS = 5
DEFAULT_MAX_WORKERS = 4


def get_config() -> dict[str, Any]:
    """Read exporter configuration from the environment (lazily, so the module stays import-safe).

    Returns:
        Dict with ``project_id``, ``raw_dataset``, ``sample_rows``,
        ``max_workers`` and ``output_dir``.

    Raises:
        RuntimeError: If ``GCP_PROJECT_ID`` or ``DBT_RAW_DATASET`` is missing.
    """
    project_id = os.getenv("GCP_PROJECT_ID")
    raw_dataset = os.getenv("DBT_RAW_DATASET")
    if not project_id or not raw_dataset:
        raise RuntimeError("Environment variables GCP_PROJECT_ID and DBT_RAW_DATASET must be set.")
    try:
        sample_rows = int(os.getenv("MOCK_SEED_SAMPLE_ROWS", str(DEFAULT_SAMPLE_ROWS)))
    except ValueError:
        sample_rows = DEFAULT_SAMPLE_ROWS
    try:
        max_workers = int(os.getenv("MOCK_SEED_MAX_WORKERS", str(DEFAULT_MAX_WORKERS)))
    except ValueError:
        max_workers = DEFAULT_MAX_WORKERS
    return {
        "project_id": project_id,
        "raw_dataset": raw_dataset,
        "sample_rows": max(sample_rows, 1),
        "max_workers": max(max_workers, 1),
        "output_dir": PROJECT_ROOT / "dbt" / "test_fixtures",
    }


def load_source_definitions(
    staging_dir: Path | None = None,
) -> list[tuple[str, str, str]]:
    """Parse all ``*_sources.yml`` files under ``dbt/models/staging``.

    Args:
        staging_dir: Override for the staging models directory (used in tests).

    Returns:
        A list of (source_name, table_name, identifier) tuples. If an identifier
        is not provided in the yaml, ``{source_name}__{table_name}`` is used.
    """
    source_defs: list[tuple[str, str, str]] = []
    staging = (
        Path(staging_dir)
        if staging_dir is not None
        else PROJECT_ROOT / "dbt" / "models" / "staging"
    )
    for yaml_path in sorted(staging.rglob("*_sources.yml")):
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # Example structure:
        # sources:
        #   - name: hubspot
        #     tables:
        #       - name: contacts
        #       - name: companies
        for source in data.get("sources", []) or []:
            source_name = (source or {}).get("name")
            for table in (source or {}).get("tables", []) or []:
                table_name = (table or {}).get("name")
                if not source_name or not table_name:
                    continue
                identifier = (table or {}).get("identifier") or f"{source_name}__{table_name}"
                source_defs.append((source_name, table_name, identifier))
    return source_defs


def json_serial(obj: Any) -> Any:
    """JSON fallback serializer for BigQuery value types (datetimes, Decimal, bytes)."""
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="ignore")
    raise TypeError(f"Type {type(obj)} not serializable")


def normalize_row_values(row: dict[str, Any]) -> dict[str, Any]:
    """Convert BigQuery row values to CSV-safe strings in a single pass.

    Nested dicts/lists become JSON strings; stringified JSON with doubled
    quotes is normalized. The row is mutated and returned.
    """
    for col, val in row.items():
        if isinstance(val, (dict, list)):
            row[col] = json.dumps(val, default=json_serial, ensure_ascii=False)
        elif isinstance(val, (datetime.datetime, datetime.date, datetime.time)):
            row[col] = val.isoformat()
        elif isinstance(val, Decimal):
            row[col] = float(val)
        elif isinstance(val, bytes):
            row[col] = val.decode("utf-8", errors="ignore")
        elif isinstance(val, str) and val.startswith(("{", "[")):
            # Normalize repeated quotes in stringified JSON fields
            row[col] = val.replace('""', '"')
    return row


def write_seed_csv(
    rows: list[dict[str, Any]],
    out_file: Path,
    dry_run: bool = False,
) -> Path | None:
    """Write scrubbed rows to a seed CSV file.

    Returns:
        The output path, or ``None`` when there is nothing to write.
    """
    if not rows:
        return None
    if dry_run:
        logger.info("[DRY-RUN] Would write %d rows: %s", len(rows), out_file)
        return out_file
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return out_file


def export_table(
    client: Any,
    source_name: str,
    identifier: str,
    *,
    project_id: str | None = None,
    raw_dataset: str | None = None,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    output_dir: Path | None = None,
    dry_run: bool = False,
) -> Path | None:
    """Export a sample of a BigQuery table to a scrubbed CSV seed file.

    Args:
        client: Authenticated BigQuery client (only ``query()`` is used).
        source_name: Name of the source (e.g. "hubspot", "notion").
        identifier: The dbt identifier for the table (e.g. "hubspot__contacts").
        project_id: GCP project override (defaults to ``GCP_PROJECT_ID``).
        raw_dataset: Raw dataset override (defaults to ``DBT_RAW_DATASET``).
        sample_rows: Number of rows to sample.
        output_dir: Seed output directory override.
        dry_run: Log the write without touching disk.

    Returns:
        The seed file path, or ``None`` when the table has no rows.
    """
    project = project_id or os.getenv("GCP_PROJECT_ID")
    dataset = raw_dataset or os.getenv("DBT_RAW_DATASET")
    out_dir = Path(output_dir) if output_dir is not None else PROJECT_ROOT / "dbt" / "test_fixtures"
    bq_table = f"{project}.{dataset}.{identifier}"
    query = f"SELECT * FROM `{bq_table}` ORDER BY RAND() LIMIT {int(sample_rows)}"
    logger.info("Exporting %s → %s.csv", bq_table, identifier)
    query_job = client.query(query)
    results = [dict(row) for row in query_job.result()]

    if not results:
        logger.info("No data found for %s", identifier)
        return None

    apply_fakes_to_rows(results)
    for row in results:
        normalize_row_values(row)

    return write_seed_csv(results, out_dir / source_name / f"{identifier}.csv", dry_run=dry_run)


def export_all(
    client: Any,
    definitions: list[tuple[str, str, str]],
    *,
    project_id: str,
    raw_dataset: str,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    output_dir: Path | None = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    dry_run: bool = False,
    source_filter: str | None = None,
) -> dict[str, list[str]]:
    """Export every (optionally source-filtered) table, with one thread per table.

    BigQuery exports are network-bound, so a small thread pool parallelizes
    them without oversubscribing the API.

    Returns:
        ``{"exported": [...identifiers], "failed": [...identifiers],
        "skipped": [...identifiers]}``.
    """
    selected = [
        (src, tbl, identifier)
        for src, tbl, identifier in definitions
        if source_filter is None or src == source_filter
    ]
    summary: dict[str, list[str]] = {"exported": [], "failed": [], "skipped": []}
    if not selected:
        logger.info("No source definitions found.")
        return summary

    workers = min(max(max_workers, 1), len(selected))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                export_table,
                client,
                src,
                identifier,
                project_id=project_id,
                raw_dataset=raw_dataset,
                sample_rows=sample_rows,
                output_dir=output_dir,
                dry_run=dry_run,
            ): identifier
            for src, _tbl, identifier in selected
        }
        for future in as_completed(futures):
            identifier = futures[future]
            try:
                result = future.result()
            except Exception as e:
                logger.warning("Failed to export %s: %s", identifier, e)
                summary["failed"].append(identifier)
            else:
                summary["exported" if result else "skipped"].append(identifier)
    return summary


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser (extracted for testability)."""
    parser = argparse.ArgumentParser(
        description="Sample BigQuery raw tables into scrubbed dbt mock seed CSVs."
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Only export tables for this source (e.g. hubspot). Default: all sources.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=None,
        help=f"Rows sampled per table (default: MOCK_SEED_SAMPLE_ROWS or {DEFAULT_SAMPLE_ROWS}).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help=f"Parallel BigQuery exports (default: MOCK_SEED_MAX_WORKERS or {DEFAULT_MAX_WORKERS}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query and scrub without writing seed files to disk",
    )
    return parser


def main(argv: list[str] | None = None) -> dict[str, list[str]]:
    """CLI entrypoint. Returns the export summary dict."""
    from google.cloud import bigquery

    args = build_parser().parse_args(argv)
    config = get_config()
    client = bigquery.Client(project=config["project_id"])
    return export_all(
        client,
        load_source_definitions(),
        project_id=config["project_id"],
        raw_dataset=config["raw_dataset"],
        sample_rows=args.sample_rows or config["sample_rows"],
        output_dir=config["output_dir"],
        max_workers=args.max_workers or config["max_workers"],
        dry_run=args.dry_run,
        source_filter=args.source,
    )


if __name__ == "__main__":
    main()
