"""
Google Health source factory moved to pipelines.sources.google_health

Prerequisite: Obtain refresh token through interactive flow.

- `Setup Health <https://developers.google.com/health/setup>`_
- Scopes:
    - https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly
    - https://www.googleapis.com/auth/googlehealth.sleep.readonly

API Resources:

- `List DataPoints <https://developers.google.com/health/reference/rest/v4/users.dataTypes.dataPoints/list>`_
- `Daily Roll Up DataPoints <https://developers.google.com/health/reference/rest/v4/users.dataTypes.dataPoints/dailyRollUp>`_
"""

from datetime import datetime
from typing import Any

import dlt
import google.auth
import requests
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator
from dlt.sources.rest_api import rest_api_source
from google.auth.transport.requests import Request
from google.cloud import secretmanager_v1
from google.oauth2.credentials import Credentials

from pipelines import SECRET_STORE


def get_google_health_token() -> str:
    """Get a Google Health access token.

    Prefer the configured OAuth refresh credentials whenever all are available,
    even if a direct access token is also configured. A direct access token is
    used only when the refresh credentials are absent; in that mode the provider
    must supply a freshly refreshed token for each run.

    This function requests a new access token from Google's OAuth2 endpoint
    using the refresh token stored in DLT secrets. It also updates the
    refresh token in Google Secret Manager or 1Password for future use if
    a new refresh token is returned by the endpoint.

    Returns:
        str: The new access token for Google Health API requests.

    Raises:
        requests.HTTPError: If the token refresh request fails.
    """
    refresh_token = dlt.secrets.get("sources.google_health.refresh_token")
    client_id = dlt.secrets.get("sources.google_health.client_id")
    client_secret = dlt.secrets.get("sources.google_health.client_secret")
    direct_token = dlt.secrets.get("sources.google_health.access_token")

    if not (refresh_token and client_id and client_secret):
        if direct_token:
            return direct_token
        # Preserve dlt's standard missing-secret diagnostics.
        refresh_token = dlt.secrets["sources.google_health.refresh_token"]
        client_id = dlt.secrets["sources.google_health.client_id"]
        client_secret = dlt.secrets["sources.google_health.client_secret"]

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
    )

    request = Request()
    creds.refresh(request)

    new_refresh = creds.refresh_token
    if new_refresh and new_refresh != dlt.secrets.get("sources.google_health.refresh_token"):
        if SECRET_STORE == "google":
            _, project_id = google.auth.default()
            client = secretmanager_v1.SecretManagerServiceClient()
            parent = client.secret_path(project_id, "sources-google-health-refresh_token")
            client.add_secret_version(
                request={
                    "parent": parent,
                    "payload": {"data": new_refresh.encode("UTF-8")},
                }
            )
        else:
            from pipelines.common.utils import update_onepassword_item

            update_onepassword_item(
                item_name="google-health",
                vault="reporting",
                field_updates={"refresh_token": new_refresh},
            )

    return creds.token


def _to_civil_datetime(value: Any) -> Any:
    """Render an incremental timestamp as an offset-less civil date-time.

    The Exercise data type only exposes ``civil_start_time`` as a server-side
    filter field, and it rejects RFC3339 values that carry a ``Z`` or numeric
    offset. The incremental cursor keeps the exact UTC timestamp for local
    comparison; this only reformats the value interpolated into the filter.
    """
    if isinstance(value, str) and value.endswith("Z"):
        return value[:-1]
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.replace(tzinfo=None).isoformat()
    return value


def google_health_source(
    access_token: str,
    initial_date: str = "1970-01-01",
    end_date: str | None = None,
    session: requests.Session | None = None,
):
    initial_ts = f"{initial_date}T00:00:00Z"
    end_ts = f"{end_date}T00:00:00Z" if end_date else None
    sleep_filter = 'sleep.interval.end_time >= "{incremental.start_value}"'
    steps_filter = 'steps.interval.start_time >= "{incremental.start_value}"'
    # Exercise only exposes a civil (offset-less) start time as a server-side
    # filter field; the UTC suffix is stripped via the incremental `convert`
    # hook while the exact UTC cursor is still used for local comparison.
    exercise_filter = 'exercise.interval.civil_start_time >= "{incremental.start_value}"'
    if end_date:
        sleep_filter += ' AND sleep.interval.end_time < "{incremental.end_value}"'
        steps_filter += ' AND steps.interval.start_time < "{incremental.end_value}"'
        exercise_filter += ' AND exercise.interval.civil_start_time < "{incremental.end_value}"'

    api_config: dict[str, Any] = {
        "client": {
            "base_url": "https://health.googleapis.com/v4/",
            "auth": {"type": "bearer", "token": access_token},
            "headers": {"Content-Type": "application/json"},
            "paginator": JSONResponseCursorPaginator(
                cursor_path="nextPageToken", cursor_param="pageToken"
            ),
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {"method": "GET", "data_selector": "dataPoints"},
        },
        "resources": [
            {
                "name": "google_health__sleep",
                "max_table_nesting": 2,
                "endpoint": {
                    "path": "users/me/dataTypes/sleep/dataPoints",
                    "params": {"filter": sleep_filter, "pageSize": 25},
                    "incremental": {
                        "cursor_path": "sleep.interval.endTime",
                        "initial_value": initial_ts,
                        "end_value": end_ts,
                    },
                },
            },
            {
                "name": "google_health__steps",
                "max_table_nesting": 4,
                "endpoint": {
                    "path": "users/me/dataTypes/steps/dataPoints",
                    "params": {"filter": steps_filter, "pageSize": 1000},
                    "incremental": {
                        "cursor_path": "steps.interval.startTime",
                        "initial_value": initial_ts,
                        "end_value": end_ts,
                    },
                },
            },
            {
                "name": "google_health__exercise",
                "max_table_nesting": 2,
                "endpoint": {
                    "path": "users/me/dataTypes/exercise/dataPoints",
                    "params": {"filter": exercise_filter, "pageSize": 25},
                    "incremental": {
                        "cursor_path": "exercise.interval.startTime",
                        "initial_value": initial_ts,
                        "end_value": end_ts,
                        "convert": _to_civil_datetime,
                    },
                },
            },
        ],
    }
    if session:
        api_config["client"]["session"] = session

    return rest_api_source(api_config)
