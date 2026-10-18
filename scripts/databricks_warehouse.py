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


def start_databricks_warehouse() -> None:
    """Start a stopped SQL warehouse and wait until it is running and healthy."""
    cli_env, warehouse_id = databricks_cli_environment()

    try:
        status = warehouse_status(cli_env, warehouse_id)
        state = status.get("state")
        health = status.get("health") or {}
        health_status = health.get("status")

        if state == "RUNNING" and health_status in (None, "HEALTHY"):
            print(f"Databricks SQL warehouse {warehouse_id} is already running and healthy.")
            return
        if state == "RUNNING" and health_status in ("DEGRADED", "FAILED"):
            detail = health.get("summary") or health.get("details") or health_status
            raise RuntimeError(f"Databricks SQL warehouse is {health_status}: {detail}")
    except subprocess.CalledProcessError:
        state = None

    if state != "STARTING":
        print(f"Starting Databricks SQL warehouse {warehouse_id} (state: {state or 'unknown'}).")
        run(["databricks", "warehouses", "start", warehouse_id, "--no-wait"], env=cli_env)

    deadline = time.monotonic() + START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL_SECONDS)
        status = warehouse_status(cli_env, warehouse_id)
        state = status.get("state")
        health = status.get("health") or {}
        health_status = health.get("status")

        if state == "RUNNING" and health_status in (None, "HEALTHY"):
            print(f"Databricks SQL warehouse {warehouse_id} is running and healthy.")
            return
        if state == "RUNNING" and health_status in ("DEGRADED", "FAILED"):
            detail = health.get("summary") or health.get("details") or health_status
            raise RuntimeError(f"Databricks SQL warehouse is {health_status}: {detail}")

    health = status.get("health") or {}
    summary = health.get("summary") or health.get("details") or ""
    raise TimeoutError(
        f"Databricks SQL warehouse did not become healthy within {START_TIMEOUT_SECONDS}s "
        f"(state: {state or 'unknown'}). {summary}"
    )


if __name__ == "__main__":
    start_databricks_warehouse()
