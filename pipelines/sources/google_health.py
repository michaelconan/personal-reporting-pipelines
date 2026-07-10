"""Google Health source factory moved to pipelines.sources.google_health"""

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator


def get_google_health_token() -> str:
    return dlt.secrets["sources.google_health.access_token"]


def google_health_source(
    access_token: str, initial_date: str = "1970-01-01", end_date: str | None = None
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
    return rest_api_source(api_config)
