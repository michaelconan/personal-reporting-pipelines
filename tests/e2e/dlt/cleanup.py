"""Cleanup helpers for Databricks end-to-end pipeline tests."""

from __future__ import annotations

from typing import Any


def cleanup_databricks_pipeline(pipeline: Any) -> None:
    """Drop the remote test schema and always clear the local dlt pipeline state."""
    pipeline_name = getattr(pipeline, "pipeline_name", "unknown")
    try:
        with pipeline.sql_client() as client:
            # dlt's SQL client supplies the catalog-qualified, dev-mode dataset name.
            dataset_name = client.fully_qualified_dataset_name()
            client.execute_sql(f"DROP SCHEMA IF EXISTS {dataset_name} CASCADE")
    except Exception as exc:
        raise RuntimeError(
            f"Failed to drop remote schema for pipeline {pipeline_name!r}: {exc}"
        ) from exc
    finally:
        # Pipeline.drop() resets local state only; it does not drop destination data.
        pipeline.drop()
