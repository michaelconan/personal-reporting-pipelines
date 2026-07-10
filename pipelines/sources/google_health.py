"""Google Health source factory moved to pipelines.sources.google_health"""

# Base
from logging import getLogger

# PyPI
import requests
from google.cloud import secretmanager_v1
import google.auth

# dlt
import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

logger = getLogger(__name__)


def get_google_health_token() -> str:
    """Get a fresh Google Health API access token using the stored refresh token.

    This function requests a new access token from Google's OAuth2 endpoint
    using the refresh token stored in DLT secrets. It also updates the
    refresh token in Google Secret Manager or 1Password for future use if it changes.

    Returns:
        str: The new access token for Google Health API requests.

    Raises:
        requests.HTTPError: If the token refresh request fails.
    """
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "grant_type": "refresh_token",
            "client_id": dlt.secrets["sources.google_health.client_id"],
            "client_secret": dlt.secrets["sources.google_health.client_secret"],
            "refresh_token": dlt.secrets["sources.google_health.refresh_token"],
        },
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()

    new_refresh_token = result.get("refresh_token")
    if new_refresh_token:
        from pipelines import SECRET_STORE

        if SECRET_STORE == "google":
            _, project_id = google.auth.default()
            client = secretmanager_v1.SecretManagerServiceClient()
            parent = client.secret_path(project_id, "sources-google_health-refresh_token")
            client.add_secret_version(
                request={
                    "parent": parent,
                    "payload": {"data": new_refresh_token.encode("UTF-8")},
                }
            )
        else:
            from pipelines.common.utils import update_onepassword_item

            update_onepassword_item(
                item_name="google_health",
                vault="reporting",
                field_updates={"refresh_token": new_refresh_token},
            )

    return result["access_token"]


def google_health_source(
    access_token: str,
    initial_date: str = "1970-01-01",
    session=None,
    end_date: str | None = None,
):
    initial_ts = f"{initial_date}T00:00:00Z"
    end_ts = f"{end_date}T00:00:00Z" if end_date else None
    api_config = {
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
                    "params": {
                        "filter": "sleep.interval.end_time >= {incremental.start_value} sleep.interval.end_time < {incremental.end_value}"
                    },
                    "incremental": {
                        "cursor_path": "sleep.interval.startTime",
                        "initial_value": initial_ts,
                        "end_value": end_ts,
                    },
                },
            },
            {
                "name": "google_health__steps",
                "max_table_nesting": 1,
                "endpoint": {
                    "path": "users/me/dataTypes/steps/dataPoints",
                    "params": {
                        "filter": "steps.interval.start_time >= {incremental.start_value} steps.interval.start_time < {incremental.end_value}"
                    },
                    "incremental": {
                        "cursor_path": "steps.interval.startTime",
                        "initial_value": initial_ts,
                        "end_value": end_ts,
                    },
                },
            },
            {
                "name": "google_health__exercise",
                "max_table_nesting": 1,
                "endpoint": {
                    "path": "users/me/dataTypes/exercise/dataPoints",
                    "params": {
                        "filter": "exercise.interval.civil_start_time >= {incremental.start_value} exercise.interval.civil_start_time < {incremental.end_value}"
                    },
                    "incremental": {
                        "cursor_path": "exercise.interval.startTime",
                        "initial_value": initial_date,
                        "end_value": end_date,
                    },
                },
            },
        ],
    }
    if session:
        api_config["client"]["session"] = session

    return rest_api_source(api_config)
