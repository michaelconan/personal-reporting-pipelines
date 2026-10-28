"""Tests for pipeline schema configuration."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
IMPORT_PIPELINES = "import pipelines; print(pipelines.RAW_SCHEMA, pipelines.DBT_SCHEMA)"
SCHEMA_ENV_VARS = (
    "DBT_TARGET",
    "DBT_SCHEMA",
    "RAW_SCHEMA",
    "DBT_SCHEMA_NAME",
    "RAW_SCHEMA_NAME",
)


def run_pipeline_config(**overrides: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for name in SCHEMA_ENV_VARS:
        env.pop(name, None)
    env.update(overrides)
    return subprocess.run(
        [sys.executable, "-c", IMPORT_PIPELINES],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_pipeline_config_defaults_to_dev_schemas():
    result = run_pipeline_config()

    assert result.returncode == 0
    assert result.stdout.strip() == "raw_dev reporting_dev"


def test_pipeline_config_treats_empty_schemas_as_unset():
    result = run_pipeline_config(DBT_SCHEMA="", RAW_SCHEMA="")

    assert result.returncode == 0
    assert result.stdout.strip() == "raw_dev reporting_dev"


def test_pipeline_config_uses_explicit_prod_schemas():
    result = run_pipeline_config(DBT_SCHEMA="reporting", RAW_SCHEMA="raw")

    assert result.returncode == 0
    assert result.stdout.strip() == "raw reporting"


def test_pipeline_config_uses_explicit_test_schemas():
    result = run_pipeline_config(DBT_SCHEMA="reporting_test", RAW_SCHEMA="raw_test")

    assert result.returncode == 0
    assert result.stdout.strip() == "raw_test reporting_test"


def test_pipeline_config_does_not_derive_schemas_from_target():
    result = run_pipeline_config(DBT_TARGET="prod")

    assert result.returncode == 0
    assert result.stdout.strip() == "raw_dev reporting_dev"
