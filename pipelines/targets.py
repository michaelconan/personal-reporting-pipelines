"""Target and schema consistency checks for pipeline runs."""

from __future__ import annotations

TARGET_SCHEMAS = {
    "dev": ("reporting_dev", "raw_dev"),
    "dev-bq": ("reporting_dev", "raw_dev"),
    "test": ("reporting_test", "raw_test"),
    "test-bq": ("reporting_test", "raw_test"),
    "prod": ("reporting", "raw"),
    "prod-bq": ("reporting", "raw"),
    "mock": ("reporting_dev", "raw_dev"),
}

INGESTION_TARGETS = {"dev", "test", "prod", "mock"}


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


def validate_ingestion_target(
    target: str | None,
    schema: str | None,
    raw_schema: str | None,
) -> None:
    validate_target(target, schema, raw_schema)
    if target not in INGESTION_TARGETS:
        choices = ", ".join(sorted(INGESTION_TARGETS))
        raise ValueError(
            f"DBT_TARGET={target} cannot be used for pipeline ingestion; choose {choices}"
        )
