"""Start the Databricks SQL warehouse configured through dlt credentials."""

from __future__ import annotations

import json
import os
import subprocess  # nosec B404
import time
from collections.abc import Sequence
from typing import Any

WAREHOUSE_PATH_PREFIX = "/sql/1.0/warehouses/"
POLL_INTERVAL_SECONDS = 10
START_TIMEOUT_SECONDS = 900
# Overall wall-clock bound for the whole start-and-wait, across all attempts
# and backoff. The test-pipelines CI job has a 20-minute timeout, so the
# start helper must never burn 6 x 15 min of per-attempt polling (~90 min)
# on a warehouse stuck in STARTING.
OVERALL_TIMEOUT_SECONDS = 900
# A STOPPED sample right after an accepted start can just be eventual
# consistency, so a launch is only declared failed after the warehouse was
# observed STARTING, or after this many consecutive STOPPED polls with no
# STARTING observed at all (~60s).
STOPPED_POLLS_WITHOUT_START = 6
# States where a start is already in flight (or winding down): wait for the
# warehouse to settle instead of issuing another start, which the API
# rejects as a non-transient error.
TRANSITIONAL_STATES = ("STARTING", "STOPPING")

# Starting a serverless SQL warehouse can fail transiently when the backing
# serverless compute pool runs out of capacity. Databricks reports this as
# RESOURCE_EXHAUSTED, surfaced by the CLI as "Cannot create the resource,
# please try again later." (Seen in scheduled runs on 2026-11-09, where the
# single start attempt failed in under 3s and killed the whole pipeline.)
# Retry only those errors, with backoff, instead of failing immediately.
START_MAX_ATTEMPTS = 6
START_RETRY_DELAYS_SECONDS = (60, 120, 240, 300, 300)
TRANSIENT_START_ERROR_MARKERS = ("cannot create the resource", "resource_exhausted")


def run(
    command: Sequence[str], *, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a command, capturing output for error reporting."""
    try:
        return subprocess.run(  # nosec B603
            command,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        detail = exc.stdout or exc.stderr or str(exc)
        raise RuntimeError(f"Databricks CLI command failed: {detail.strip()}") from exc


def databricks_cli_environment() -> tuple[dict[str, str], str]:
    """Map dlt's Databricks credentials to CLI variables and extract the warehouse ID."""
    host = os.environ.get("DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME")
    http_path = os.environ.get("DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH")
    access_token = os.environ.get("DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN")
    client_id = os.environ.get("DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID")
    client_secret = os.environ.get("DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET")

    if not host or not http_path:
        raise SystemExit(
            "Databricks startup needs dlt environment variables: "
            "DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME and "
            "DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH."
        )
    if not http_path.startswith(WAREHOUSE_PATH_PREFIX):
        raise SystemExit(
            "DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH must be a SQL warehouse path "
            f"beginning with {WAREHOUSE_PATH_PREFIX}"
        )
    if bool(client_id) != bool(client_secret):
        raise SystemExit(
            "Databricks OAuth M2M authentication requires both "
            "DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID and "
            "DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET."
        )
    if not access_token and not (client_id and client_secret):
        raise SystemExit(
            "Databricks startup needs either "
            "DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN or both "
            "DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID and "
            "DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET."
        )

    warehouse_id = http_path.removeprefix(WAREHOUSE_PATH_PREFIX).strip("/")
    if not warehouse_id or "/" in warehouse_id:
        raise SystemExit("Could not extract a warehouse ID from the dlt HTTP path.")

    cli_env = os.environ.copy()
    cli_env["DATABRICKS_HOST"] = host if host.startswith("https://") else f"https://{host}"
    if client_id and client_secret:
        # These are Databricks unified authentication's native CLI environment variables.
        cli_env["DATABRICKS_CLIENT_ID"] = client_id
        cli_env["DATABRICKS_CLIENT_SECRET"] = client_secret
        cli_env["DATABRICKS_AUTH_TYPE"] = "oauth-m2m"
        cli_env.pop("DATABRICKS_TOKEN", None)
    else:
        cli_env["DATABRICKS_TOKEN"] = access_token or ""
        cli_env["DATABRICKS_AUTH_TYPE"] = "pat"
    return cli_env, warehouse_id


def warehouse_status(cli_env: dict[str, str], warehouse_id: str) -> dict[str, Any]:
    """Read the SQL warehouse state through the Databricks CLI."""
    result = subprocess.run(  # nosec B603 B607
        ["databricks", "warehouses", "get", warehouse_id, "--output", "json"],
        env=cli_env,
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Databricks CLI returned invalid JSON for warehouse status.") from exc


def is_transient_start_error(error: Exception) -> bool:
    """Return True when a start failure is worth retrying.

    Only serverless capacity exhaustion is retried. Anything else (auth,
    permissions, an unknown warehouse, bad configuration) fails immediately
    so a real problem is not hidden behind a long retry loop.
    """
    message = str(error).lower()
    return any(marker in message for marker in TRANSIENT_START_ERROR_MARKERS)


def _retry_delay_seconds(attempt: int) -> int:
    """Backoff before the next start attempt (1-based attempt just made)."""
    index = min(attempt - 1, len(START_RETRY_DELAYS_SECONDS) - 1)
    return START_RETRY_DELAYS_SECONDS[index]


def _state_and_health(status: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return (state, health status), raising if running but unhealthy."""
    state = status.get("state")
    health = status.get("health") or {}
    health_status = health.get("status")
    if state == "RUNNING" and health_status in ("DEGRADED", "FAILED"):
        detail = health.get("summary") or health.get("details") or health_status
        raise RuntimeError(f"Databricks SQL warehouse is {health_status}: {detail}")
    return state, health_status


def _warehouse_state(status: dict[str, Any]) -> str | None:
    """Return the warehouse state, raising if it is running but unhealthy."""
    state, _ = _state_and_health(status)
    return state


def _is_running_and_healthy(status: dict[str, Any]) -> bool:
    """Return True when the warehouse is running with no health problem."""
    state, health_status = _state_and_health(status)
    return state == "RUNNING" and health_status in (None, "HEALTHY")


def _sleep_before_retry(delay: int, overall_deadline: float, warehouse_id: str) -> None:
    """Sleep for the backoff delay, or fail fast if the overall deadline hits."""
    remaining = overall_deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError(
            f"Databricks SQL warehouse {warehouse_id} did not start "
            f"within {OVERALL_TIMEOUT_SECONDS}s."
        )
    time.sleep(min(delay, remaining))


def start_databricks_warehouse() -> None:
    """Start a stopped SQL warehouse and wait until it is running and healthy.

    A start rejected with a transient capacity error is retried with
    backoff (up to START_MAX_ATTEMPTS starts). A start that is accepted but
    then falls back to STOPPED while polling is treated the same way: the
    launch itself failed, so start again rather than polling a stopped
    warehouse until the timeout. The whole operation is bounded by
    OVERALL_TIMEOUT_SECONDS (15 minutes), so a warehouse stuck in STARTING
    cannot burn a fresh 15-minute poll on every attempt.
    """
    cli_env, warehouse_id = databricks_cli_environment()
    overall_deadline = time.monotonic() + OVERALL_TIMEOUT_SECONDS

    for attempt in range(1, START_MAX_ATTEMPTS + 1):
        try:
            status = warehouse_status(cli_env, warehouse_id)
            state = _warehouse_state(status)
            if _is_running_and_healthy(status):
                print(f"Databricks SQL warehouse {warehouse_id} is already running and healthy.")
                return
        except subprocess.CalledProcessError:
            status = {}
            state = None

        launched = state in TRANSITIONAL_STATES
        if state not in TRANSITIONAL_STATES:
            print(
                f"Starting Databricks SQL warehouse {warehouse_id} "
                f"(state: {state or 'unknown'}, attempt {attempt}/{START_MAX_ATTEMPTS})."
            )
            try:
                run(["databricks", "warehouses", "start", warehouse_id, "--no-wait"], env=cli_env)
            except RuntimeError as exc:
                if not is_transient_start_error(exc) or attempt == START_MAX_ATTEMPTS:
                    raise
                delay = _retry_delay_seconds(attempt)
                print(
                    f"Databricks SQL warehouse start failed with a transient error "
                    f"(attempt {attempt}/{START_MAX_ATTEMPTS}): {exc} "
                    f"Retrying in {delay}s."
                )
                _sleep_before_retry(delay, overall_deadline, warehouse_id)
                continue
            launched = True

        deadline = min(time.monotonic() + START_TIMEOUT_SECONDS, overall_deadline)
        start_failed = False
        stopped_without_start = 0
        while time.monotonic() < deadline:
            time.sleep(POLL_INTERVAL_SECONDS)
            try:
                status = warehouse_status(cli_env, warehouse_id)
            except (subprocess.CalledProcessError, RuntimeError) as exc:
                # A transient failure reading status must not kill the run
                # the way the start retry logic already tolerates transient
                # failures; keep polling until the deadline.
                print(f"Could not read Databricks SQL warehouse status: {exc}. Retrying.")
                continue
            state = _warehouse_state(status)

            if _is_running_and_healthy(status):
                print(f"Databricks SQL warehouse {warehouse_id} is running and healthy.")
                return
            if state in TRANSITIONAL_STATES:
                launched = True
                stopped_without_start = 0
                continue
            if state == "STOPPED":
                if launched:
                    # The start was accepted and the launch actually began,
                    # then the warehouse stopped again; retry the start.
                    print(
                        f"Databricks SQL warehouse {warehouse_id} stopped while starting "
                        f"(attempt {attempt}/{START_MAX_ATTEMPTS})."
                    )
                    start_failed = True
                    break
                stopped_without_start += 1
                if stopped_without_start >= STOPPED_POLLS_WITHOUT_START:
                    # ~60s of STOPPED with no STARTING observed: the launch
                    # failed outright, not eventual consistency.
                    print(
                        f"Databricks SQL warehouse {warehouse_id} never left STOPPED "
                        f"(attempt {attempt}/{START_MAX_ATTEMPTS})."
                    )
                    start_failed = True
                    break
                continue

        if start_failed:
            if attempt == START_MAX_ATTEMPTS:
                raise RuntimeError(
                    f"Databricks SQL warehouse {warehouse_id} did not start "
                    f"after {START_MAX_ATTEMPTS} attempts."
                )
            delay = _retry_delay_seconds(attempt)
            print(
                f"Retrying Databricks SQL warehouse start in {delay}s "
                f"(attempt {attempt + 1}/{START_MAX_ATTEMPTS})."
            )
            _sleep_before_retry(delay, overall_deadline, warehouse_id)
            continue

        health = status.get("health") or {}
        summary = health.get("summary") or health.get("details") or ""
        raise TimeoutError(
            f"Databricks SQL warehouse did not become healthy within {OVERALL_TIMEOUT_SECONDS}s "
            f"(state: {state or 'unknown'}). {summary}"
        )


if __name__ == "__main__":
    start_databricks_warehouse()
