"""Tests for target/schema environment validation."""

from __future__ import annotations

import pytest

from scripts.validate_target import validate_target


@pytest.mark.parametrize(
    ("target", "schema", "raw_schema"),
    [
        ("dev", "reporting_dev", "raw_dev"),
        ("test", "reporting_test", "raw_test"),
        ("prod", "reporting", "raw"),
    ],
)
def test_validate_target_accepts_matching_schemas(target, schema, raw_schema):
    validate_target(target, schema, raw_schema)


def test_validate_target_rejects_unknown_target():
    with pytest.raises(ValueError, match="DBT_TARGET must be one of"):
        validate_target("mock", "reporting_mock", "raw_mock")


def test_validate_target_rejects_mismatched_schemas():
    with pytest.raises(ValueError, match="requires DBT_SCHEMA=reporting"):
        validate_target("prod", "reporting_dev", "raw_dev")
