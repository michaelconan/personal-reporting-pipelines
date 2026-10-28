"""Validate that the dbt target and schema environment variables agree."""

from __future__ import annotations

import os

TARGET_SCHEMAS = {
    "dev": ("reporting_dev", "raw_dev"),
    "test": ("reporting_test", "raw_test"),
    "prod": ("reporting", "raw"),
}


def validate_target(
    target: str | None,
    schema: str | None,
    raw_schema: str | None,
) -> None:
    expected = TARGET_SCHEMAS.get(target or "")
    if expected is None:
        choices = ", ".join(sorted(TARGET_SCHEMAS))
        raise ValueError(f"DBT_TARGET must be one of {choices} (got {target!r})")

    expected_schema, expected_raw_schema = expected
    if (schema, raw_schema) != (expected_schema, expected_raw_schema):
        raise ValueError(
            f"DBT_TARGET={target} requires DBT_SCHEMA={expected_schema} and "
            f"RAW_SCHEMA={expected_raw_schema}; got DBT_SCHEMA={schema!r} and "
            f"RAW_SCHEMA={raw_schema!r}"
        )


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
