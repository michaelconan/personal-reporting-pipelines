"""Tests for Databricks e2e pipeline cleanup."""

from __future__ import annotations

from typing import Self

import pytest

from tests.e2e.dlt.cleanup import cleanup_databricks_pipeline


class StubSqlClient:
    def __init__(self, events: list[object], *, error: Exception | None = None) -> None:
        self.events = events
        self.error = error

    def __enter__(self) -> Self:
        self.events.append("client_enter")
        return self

    def __exit__(self, *_: object) -> None:
        self.events.append("client_exit")

    def fully_qualified_dataset_name(self) -> str:
        self.events.append("qualified_dataset_name")
        return "`test_catalog`.`live_data_live_e2e_test`"

    def execute_sql(self, statement: str) -> None:
        self.events.append(statement)
        if self.error:
            raise self.error


class StubPipeline:
    def __init__(self, events: list[object], *, sql_error: Exception | None = None) -> None:
        self.events = events
        self.client = StubSqlClient(events, error=sql_error)

    def sql_client(self) -> StubSqlClient:
        self.events.append("sql_client")
        return self.client

    def drop(self) -> None:
        self.events.append("pipeline_drop")


def test_cleanup_drops_qualified_remote_schema_before_local_pipeline_state() -> None:
    events: list[object] = []
    pipeline = StubPipeline(events)

    cleanup_databricks_pipeline(pipeline)

    statement = "DROP SCHEMA IF EXISTS `test_catalog`.`live_data_live_e2e_test` CASCADE"
    assert statement in events
    assert events.index(statement) < events.index("pipeline_drop")


def test_cleanup_resets_local_state_even_when_schema_drop_fails() -> None:
    events: list[object] = []
    pipeline = StubPipeline(events, sql_error=RuntimeError("drop failed"))

    with pytest.raises(RuntimeError, match="drop failed"):
        cleanup_databricks_pipeline(pipeline)

    assert events[-1] == "pipeline_drop"
