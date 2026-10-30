"""Validate that the dbt target and schema environment variables agree."""

from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.targets import validate_target  # noqa: E402


def main() -> int:
    try:
        validate_target(
            os.getenv("DBT_TARGET"),
            os.getenv("DBT_SCHEMA"),
            os.getenv("RAW_SCHEMA"),
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
