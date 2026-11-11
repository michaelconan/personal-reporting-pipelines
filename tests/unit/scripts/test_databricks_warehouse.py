"""Tests for Databricks warehouse startup and CLI authentication mapping."""

from __future__ import annotations

import subprocess

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
    statuses = [
        {"state": "STOPPED"},
        {"state": "STARTING"},
        {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    ]
    commands = []
    monkeypatch.setattr(
        databricks_warehouse, "databricks_cli_environment", lambda: ({}, "warehouse-id")
    )
    monkeypatch.setattr(databricks_warehouse, "warehouse_status", _status_provider(statuses))
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


def _status_provider(statuses):
    """Yield statuses in order, then keep returning the last one.

    A plain iterator raises StopIteration if the code reads one status too
    many, which surfaces as an opaque test failure; repeating the last
    status keeps the failure (if any) in the code under test.
    """
    iterator = iter(statuses)
    last = [statuses[-1]]

    def _next(*_):
        try:
            last[0] = next(iterator)
        except StopIteration:
            pass
        return last[0]

    return _next


def _patch_start(monkeypatch, statuses, run) -> None:
    monkeypatch.setattr(
        databricks_warehouse, "databricks_cli_environment", lambda: ({}, "warehouse-id")
    )
    monkeypatch.setattr(databricks_warehouse, "warehouse_status", _status_provider(statuses))
    monkeypatch.setattr(databricks_warehouse, "run", run)
    monkeypatch.setattr(databricks_warehouse.time, "sleep", lambda _: None)


def _record_sleeps(monkeypatch):
    """Replace time.sleep with a recorder; return the recorded durations."""
    sleeps = []
    monkeypatch.setattr(databricks_warehouse.time, "sleep", sleeps.append)
    return sleeps


def test_retry_delay_seconds() -> None:
    # Only attempts 1-5 ever produce a delay: attempt 6 raises instead of
    # sleeping, so no value for attempt 6 can occur.
    assert [databricks_warehouse._retry_delay_seconds(a) for a in range(1, 6)] == [
        60,
        120,
        240,
        300,
        300,
    ]


def test_start_error_classification() -> None:
    assert databricks_warehouse.is_transient_start_error(
        RuntimeError("Databricks CLI command timed out after 120s")
    )
    # A generic timeout is not a capacity error: only run()'s unique
    # "command timed out after" message is transient.
    assert not databricks_warehouse.is_transient_start_error(RuntimeError("connection timed out"))
    assert databricks_warehouse.is_benign_start_error(
        RuntimeError("Error: cannot start warehouse: already starting")
    )
    assert not databricks_warehouse.is_benign_start_error(
        RuntimeError("Databricks CLI command failed: Error: PERMISSION_DENIED.")
    )


def test_state_and_health_raises_for_unhealthy_running() -> None:
    with pytest.raises(RuntimeError, match="UNHEALTHY"):
        databricks_warehouse._state_and_health(
            {"state": "RUNNING", "health": {"status": "UNHEALTHY", "summary": "disk full"}}
        )


def test_state_and_health_tolerates_malformed_health_payload() -> None:
    # A non-dict health payload must not crash with AttributeError.
    assert databricks_warehouse._state_and_health({"state": "RUNNING", "health": "weird"}) == (
        "RUNNING",
        None,
    )


def test_warehouse_status_rejects_non_object_json(monkeypatch) -> None:
    class FakeResult:
        stdout = "[1, 2]"

    monkeypatch.setattr(databricks_warehouse, "_run_cli", lambda *_, **__: FakeResult())

    with pytest.raises(TypeError, match="unexpected warehouse status"):
        databricks_warehouse.warehouse_status({}, "warehouse-id")


def test_start_warehouse_fails_fast_on_deleted_warehouse(monkeypatch) -> None:
    _patch_start(
        monkeypatch,
        [{"state": "DELETED"}],
        lambda command, **_: pytest.fail("start must not be issued for a deleted warehouse"),
    )

    with pytest.raises(RuntimeError, match="cannot be started"):
        databricks_warehouse.start_databricks_warehouse()


def test_poll_for_healthy_fails_fast_on_deleting(monkeypatch) -> None:
    monkeypatch.setattr(
        databricks_warehouse,
        "warehouse_status",
        lambda *_: {"state": "DELETING"},
    )
    monkeypatch.setattr(databricks_warehouse.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="cannot be started"):
        databricks_warehouse._poll_for_healthy(
            {}, "warehouse-id", databricks_warehouse.time.monotonic() + 900.0, False
        )


def test_settle_stopping_returns_healthy_when_warehouse_starts_on_its_own(
    monkeypatch,
) -> None:
    statuses = [
        {"state": "STOPPING"},
        {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    ]
    commands = []
    _patch_start(
        monkeypatch,
        statuses,
        lambda command, **_: commands.append(command),
    )

    databricks_warehouse.start_databricks_warehouse()

    # The warehouse became healthy while settling, so no start was issued.
    assert commands == []


def test_start_warehouse_retries_transient_capacity_error(monkeypatch) -> None:
    # The 2026-11-09 failure: start rejected with "Cannot create the
    # resource, please try again later." (RESOURCE_EXHAUSTED) while STOPPED.
    statuses = [
        {"state": "STOPPED"},
        {"state": "STOPPED"},
        {"state": "STARTING"},
        {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    ]
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
    assert commands[1] == ["databricks", "warehouses", "start", "warehouse-id", "--no-wait"]


def test_start_warehouse_does_not_retry_non_transient_error(monkeypatch) -> None:
    statuses = [{"state": "STOPPED"}]
    commands = []

    def failing_run(command, **_):
        commands.append(command)
        raise RuntimeError("Databricks CLI command failed: Error: PERMISSION_DENIED.")

    _patch_start(monkeypatch, statuses, failing_run)

    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 1


def test_start_warehouse_gives_up_after_max_transient_attempts(monkeypatch) -> None:
    statuses = [{"state": "STOPPED"}] * databricks_warehouse.START_MAX_ATTEMPTS
    commands = []

    def failing_run(command, **_):
        commands.append(command)
        raise RuntimeError(
            "Databricks CLI command failed: "
            "RESOURCE_EXHAUSTED: Cannot create the resource, please try again later."
        )

    _patch_start(monkeypatch, statuses, failing_run)

    with pytest.raises(RuntimeError, match="did not start after 6 attempts"):
        databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == databricks_warehouse.START_MAX_ATTEMPTS


def test_start_warehouse_retries_when_it_stops_while_starting(monkeypatch) -> None:
    # Start accepted, the launch actually began (STARTING observed), then
    # the warehouse fell back to STOPPED; the start itself must be issued
    # a second time. The old fixture never issued a second start, so it
    # passed without covering the retry.
    statuses = [
        {"state": "STOPPED"},
        {"state": "STARTING"},
        {"state": "STOPPED"},
        {"state": "STOPPED"},
        {"state": "STARTING"},
        {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    ]
    commands = []
    _patch_start(
        monkeypatch,
        statuses,
        lambda command, **_: commands.append(command),
    )

    databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 2
    assert commands[1] == ["databricks", "warehouses", "start", "warehouse-id", "--no-wait"]


def test_start_warehouse_waits_out_stopping(monkeypatch) -> None:
    # A STOPPING warehouse must not trigger an immediate start (the API
    # rejects it as non-transient); poll until it settles, then start it
    # in the same attempt — no backoff, no attempt burned.
    statuses = [
        {"state": "STOPPING"},
        {"state": "STOPPED"},
        {"state": "STOPPED"},
        {"state": "STARTING"},
        {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    ]
    commands = []
    _patch_start(
        monkeypatch,
        statuses,
        lambda command, **_: commands.append(command),
    )
    sleeps = _record_sleeps(monkeypatch)

    databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 1
    assert 60 not in sleeps
    assert all(sleep == databricks_warehouse.POLL_INTERVAL_SECONDS for sleep in sleeps)


def test_start_warehouse_tolerates_stopped_samples_after_accepted_start(monkeypatch) -> None:
    # Grace window: STOPPED polls right after an accepted start are
    # eventual consistency, not a failed launch — a single start, no
    # retry delay, once STARTING is observed.
    statuses = [
        {"state": "STOPPED"},
        {"state": "STOPPED"},
        {"state": "STOPPED"},
        {"state": "STOPPED"},
        {"state": "STARTING"},
        {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    ]
    commands = []
    _patch_start(
        monkeypatch,
        statuses,
        lambda command, **_: commands.append(command),
    )
    sleeps = _record_sleeps(monkeypatch)

    databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 1
    assert 60 not in sleeps
    assert all(sleep == databricks_warehouse.POLL_INTERVAL_SECONDS for sleep in sleeps)


def test_start_warehouse_retries_after_grace_window_expires(monkeypatch) -> None:
    # Boundary test: STOPPED_POLLS_WITHOUT_START consecutive STOPPED
    # polls with no STARTING observed must issue a second start.
    grace = databricks_warehouse.STOPPED_POLLS_WITHOUT_START
    statuses = (
        [{"state": "STOPPED"}]
        + [{"state": "STOPPED"}] * grace
        + [
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

    assert len(commands) == 2
    assert commands[1] == ["databricks", "warehouses", "start", "warehouse-id", "--no-wait"]


def test_start_warehouse_treats_already_starting_as_benign(monkeypatch) -> None:
    # A duplicate start rejected as "already starting" must not fail the
    # run: a start is in flight, so poll for it instead.
    statuses = [
        {"state": "STOPPED"},
        {"state": "STARTING"},
        {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    ]
    commands = []

    def benign_run(command, **_):
        commands.append(command)
        raise RuntimeError("Databricks CLI command failed: Error: already starting.")

    _patch_start(monkeypatch, statuses, benign_run)

    databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 1


def test_settle_stopping_tolerates_transient_status_read_failure(monkeypatch) -> None:
    statuses = [
        {"state": "STOPPING"},
        {"state": "STOPPED"},
        {"state": "STARTING"},
        {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    ]
    provider = _status_provider(statuses)
    calls = {"count": 0}

    def status_with_one_failure(*_):
        calls["count"] += 1
        if calls["count"] == 2:
            raise subprocess.CalledProcessError(1, ["databricks"])
        return provider(*_)

    monkeypatch.setattr(
        databricks_warehouse, "databricks_cli_environment", lambda: ({}, "warehouse-id")
    )
    monkeypatch.setattr(databricks_warehouse, "warehouse_status", status_with_one_failure)
    monkeypatch.setattr(databricks_warehouse.time, "sleep", lambda _: None)
    commands = []
    monkeypatch.setattr(databricks_warehouse, "run", lambda command, **_: commands.append(command))

    databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 1


def test_settle_stopping_times_out(monkeypatch) -> None:
    monkeypatch.setattr(
        databricks_warehouse,
        "warehouse_status",
        lambda *_: {"state": "STOPPING"},
    )
    monkeypatch.setattr(databricks_warehouse.time, "sleep", lambda _: None)
    clock = [0.0]

    def fake_monotonic():
        value = clock[0]
        clock[0] += 10000.0
        return value

    monkeypatch.setattr(databricks_warehouse.time, "monotonic", fake_monotonic)

    with pytest.raises(TimeoutError, match="did not stop"):
        databricks_warehouse._settle_stopping({}, "warehouse-id", 900.0)


def test_start_warehouse_fails_fast_when_initial_status_read_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        databricks_warehouse, "databricks_cli_environment", lambda: ({}, "warehouse-id")
    )

    def failing_status(*_):
        raise subprocess.CalledProcessError(1, ["databricks"], stderr="PERMISSION_DENIED")

    monkeypatch.setattr(databricks_warehouse, "warehouse_status", failing_status)
    commands = []
    monkeypatch.setattr(databricks_warehouse, "run", lambda command, **_: commands.append(command))

    with pytest.raises(RuntimeError, match="Could not read"):
        databricks_warehouse.start_databricks_warehouse()

    assert commands == []


def test_start_warehouse_tolerates_transient_status_poll_failure(monkeypatch) -> None:
    statuses = [
        {"state": "STOPPED"},
        {"state": "STARTING"},
        {"state": "RUNNING", "health": {"status": "HEALTHY"}},
    ]
    provider = _status_provider(statuses)
    poll_calls = {"count": 0}

    def status_with_one_failure(*_):
        poll_calls["count"] += 1
        if poll_calls["count"] == 2:
            raise subprocess.CalledProcessError(1, ["databricks"])
        return provider(*_)

    monkeypatch.setattr(
        databricks_warehouse, "databricks_cli_environment", lambda: ({}, "warehouse-id")
    )
    monkeypatch.setattr(databricks_warehouse, "warehouse_status", status_with_one_failure)
    monkeypatch.setattr(databricks_warehouse.time, "sleep", lambda _: None)
    commands = []
    monkeypatch.setattr(databricks_warehouse, "run", lambda command, **_: commands.append(command))

    databricks_warehouse.start_databricks_warehouse()

    assert len(commands) == 1


def test_start_warehouse_overall_deadline_bounds_stuck_starting(monkeypatch) -> None:
    # A warehouse stuck in STARTING must fail within the overall deadline,
    # not after a fresh 15-minute poll on every attempt. The fake clock
    # advances slowly (10s per read), so the poll loop actually runs and
    # reads status many times before the deadline hits.
    status_reads = {"count": 0}

    def stuck_starting(*_):
        status_reads["count"] += 1
        return {"state": "STARTING"}

    monkeypatch.setattr(
        databricks_warehouse, "databricks_cli_environment", lambda: ({}, "warehouse-id")
    )
    monkeypatch.setattr(databricks_warehouse, "warehouse_status", stuck_starting)
    monkeypatch.setattr(databricks_warehouse, "run", lambda command, **_: None)
    monkeypatch.setattr(databricks_warehouse.time, "sleep", lambda _: None)
    clock = [0.0]

    def fake_monotonic():
        value = clock[0]
        clock[0] += 10.0
        return value

    monkeypatch.setattr(databricks_warehouse.time, "monotonic", fake_monotonic)

    with pytest.raises(TimeoutError, match="900"):
        databricks_warehouse.start_databricks_warehouse()

    # The poll loop must have actually run: status was read many times
    # before the deadline, not zero times — and the deadline must bound
    # it: with a 10s step, ~90 reads is the ceiling (900s / 10s), plus a
    # small slack for the initial read and loop guards.
    assert 1 < status_reads["count"] <= 95


def test_sleep_before_retry_fails_when_deadline_passed(monkeypatch) -> None:
    # Drives _sleep_before_retry's remaining <= 0 branch directly: the
    # clock is already past the deadline when a retry sleep is requested.
    monkeypatch.setattr(databricks_warehouse.time, "monotonic", lambda: 1000.0)

    with pytest.raises(TimeoutError, match="did not start"):
        databricks_warehouse._sleep_before_retry(60, 900.0, "warehouse-id")
