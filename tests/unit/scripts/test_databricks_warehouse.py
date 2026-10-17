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
