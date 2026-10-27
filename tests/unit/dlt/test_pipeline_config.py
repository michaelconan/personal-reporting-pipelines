"""Tests for pipeline target configuration."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
IMPORT_PIPELINES = "import pipelines; print(pipelines.RAW_SCHEMA, pipelines.DBT_SCHEMA)"


def run_pipeline_config(target: str | None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if target is None:
        env.pop("DBT_TARGET", None)
    else:
        env["DBT_TARGET"] = target
    return subprocess.run(
        [sys.executable, "-c", IMPORT_PIPELINES],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_pipeline_config_requires_target():
    result = run_pipeline_config(None)

    assert result.returncode != 0
    assert "DBT_TARGET must be set" in result.stderr


def test_pipeline_config_uses_prod_schemas_without_suffix():
    result = run_pipeline_config("prod")

    assert result.returncode == 0
    assert result.stdout.strip() == "raw reporting"


def test_pipeline_config_suffixes_non_prod_schemas():
    result = run_pipeline_config("test")

    assert result.returncode == 0
    assert result.stdout.strip() == "raw_test reporting_test"
