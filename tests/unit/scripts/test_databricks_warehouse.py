"""Tests for Databricks warehouse startup and CLI authentication mapping."""

from __future__ import annotations

import pytest

from scripts import databricks_warehouse


def test_databricks_cli_environment_uses_dlt_credentials(monkeypatch) -> None:
    monkeypatch.setenv(
        "DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME",
        "dbc.example.test",
    )
    monkeypatch.setenv(
        "DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH",
        "/sql/1.0/warehouses/warehouse-id",
    )
    monkeypatch.setenv("DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN", "test-token")

    cli_env, warehouse_id = databricks_warehouse.databricks_cli_environment()

    assert cli_env["DATABRICKS_HOST"] == "https://dbc.example.test"
    assert cli_env["DATABRICKS_TOKEN"] == "test-token"
    assert cli_env["DATABRICKS_AUTH_TYPE"] == "pat"
    assert warehouse_id == "warehouse-id"


def test_databricks_cli_environment_uses_dlt_service_principal_credentials(monkeypatch) -> None:
    monkeypatch.setenv(
        "DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME",
        "dbc.example.test",
    )
    monkeypatch.setenv(
        "DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH",
        "/sql/1.0/warehouses/warehouse-id",
    )
    monkeypatch.setenv("DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID", "client-id")
    monkeypatch.setenv("DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET", "client-secret")

    cli_env, warehouse_id = databricks_warehouse.databricks_cli_environment()

    assert cli_env["DATABRICKS_HOST"] == "https://dbc.example.test"
    assert cli_env["DATABRICKS_CLIENT_ID"] == "client-id"
    assert cli_env["DATABRICKS_CLIENT_SECRET"] == "client-secret"
    assert cli_env["DATABRICKS_AUTH_TYPE"] == "oauth-m2m"
    assert "DATABRICKS_TOKEN" not in cli_env
    assert warehouse_id == "warehouse-id"


def test_databricks_cli_environment_rejects_partial_service_principal_credentials(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME",
        "dbc.example.test",
    )
    monkeypatch.setenv(
        "DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH",
        "/sql/1.0/warehouses/warehouse-id",
    )
    monkeypatch.setenv("DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID", "client-id")
    monkeypatch.delenv("DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET", raising=False)

    with pytest.raises(SystemExit, match="requires both"):
        databricks_warehouse.databricks_cli_environment()


def test_start_warehouse_starts_then_waits_for_healthy(monkeypatch) -> None:
    statuses = iter(
        [
            {"state": "STOPPED"},
            {"state": "STARTING"},
            {"state": "RUNNING", "health": {"status": "HEALTHY"}},
        ]
    )
    commands = []
    monkeypatch.setattr(
        databricks_warehouse, "databricks_cli_environment", lambda: ({}, "warehouse-id")
    )
    monkeypatch.setattr(databricks_warehouse, "warehouse_status", lambda *_: next(statuses))
    monkeypatch.setattr(databricks_warehouse, "run", lambda command, **_: commands.append(command))
    monkeypatch.setattr(databricks_warehouse.time, "sleep", lambda _: None)

    databricks_warehouse.start_databricks_warehouse()

    assert commands == [["databricks", "warehouses", "start", "warehouse-id", "--no-wait"]]


def test_start_warehouse_is_idempotent_when_running_and_healthy(monkeypatch) -> None:
    commands = []
    monkeypatch.setattr(
        databricks_warehouse, "databricks_cli_environment", lambda: ({}, "warehouse-id")
    )
    monkeypatch.setattr(
        databricks_warehouse,
        "warehouse_status",
        lambda *_: {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    )
    monkeypatch.setattr(databricks_warehouse, "run", lambda command, **_: commands.append(command))

    databricks_warehouse.start_databricks_warehouse()

    assert commands == []


def _patch_start(monkeypatch, statuses, run) -> None:
    monkeypatch.setattr(
        databricks_warehouse, "databricks_cli_environment", lambda: ({}, "warehouse-id")
    )
    monkeypatch.setattr(databricks_warehouse, "warehouse_status", lambda *_: next(statuses))
    monkeypatch.setattr(databricks_warehouse, "run", run)
    monkeypatch.setattr(databricks_warehouse.time, "sleep", lambda _: None)


def test_start_warehouse_retries_transient_capacity_error(monkeypatch) -> None:
    # The 2026-11-09 failure: start rejected with "Cannot create the
    # resource, please try again later." (RESOURCE_EXHAUSTED) while STOPPED.
    statuses = iter(
        [
            {"state": "STOPPED"},
            {"state": "STOPPED"},
            {"state": "STARTING"},
            {"state": "RUNNING", "health": {"status": "HEALTHY"}},
        ]
    )
    commands = []

    def flaky_run(command, **_):
        commands.append(command)
        if len(commands) == 1:
            raise RuntimeError(
                "Databricks CLI command failed: "
                "Error: Cannot create the resource, please try again later."
            )

    _patch_start(monkeypatch, statuses, flaky_run)

    databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 2
    assert commands[0] == ["databricks", "warehouses", "start", "warehouse-id", "--no-wait"]


def test_start_warehouse_does_not_retry_non_transient_error(monkeypatch) -> None:
    statuses = iter([{"state": "STOPPED"}])
    commands = []

    def failing_run(command, **_):
        commands.append(command)
        raise RuntimeError("Databricks CLI command failed: Error: PERMISSION_DENIED.")

    _patch_start(monkeypatch, statuses, failing_run)

    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 1


def test_start_warehouse_gives_up_after_max_transient_attempts(monkeypatch) -> None:
    statuses = iter([{"state": "STOPPED"}] * databricks_warehouse.START_MAX_ATTEMPTS)
    commands = []

    def failing_run(command, **_):
        commands.append(command)
        raise RuntimeError(
            "Databricks CLI command failed: "
            "RESOURCE_EXHAUSTED: Cannot create the resource, please try again later."
        )

    _patch_start(monkeypatch, statuses, failing_run)

    with pytest.raises(RuntimeError, match="Cannot create the resource"):
        databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == databricks_warehouse.START_MAX_ATTEMPTS


def test_start_warehouse_retries_when_it_stops_while_starting(monkeypatch) -> None:
    # Start accepted, but the launch failed and the warehouse fell back to
    # STOPPED while polling; the start itself should be retried.
    statuses = iter(
        [
            {"state": "STOPPED"},
            {"state": "STOPPED"},
            {"state": "STARTING"},
            {"state": "RUNNING", "health": {"status": "HEALTHY"}},
        ]
    )
    commands = []
    _patch_start(
        monkeypatch,
        statuses,
        lambda command, **_: commands.append(command),
    )

    databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 1
