import pendulum

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.destinations import bigquery
from dlt.helpers.airflow_helper import PipelineTasksGroup
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

# Airflow hooks and operators
from airflow.decorators import dag
from airflow.hooks.base import BaseHook

# Common custom tasks
from dags.michael import RAW_SCHEMA, IS_TEST

# Connection IDs
HUBSPOT_CONN_ID = "hubspot_app"
BQ_CONN_ID = "bigquery_reporting"


def get_hubspot_source(db_name: str, api_key: str, initial_date: str = "2024-01-01"):

    # Set default incremental dates
    # Should be overriden by Airflow data interval
    initial_dt = pendulum.parse(initial_date)
    if IS_TEST:
        end_dt = initial_dt.add(days=7)
    else:
        end_dt = pendulum.now("UTC")

    CRM_OBJECTS = {
        "contacts": {
            "filter_key": "lastmodifieddate",
        },
        "companies": {
            "filter_key": "hs_lastmodifieddate",
        },
    }

    api_config = {
        "client": {
            "base_url": "https://api.hubapi.com/",
            "auth": {
                "type": "bearer",
                "token": api_key,
            },
            "headers": {
                "Content-Type": "application/json",
            },
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {
                "method": "POST",
                "paginator": JSONResponseCursorPaginator(
                    cursor_path="next_cursor",
                    cursor_body_path="start_cursor",
                ),
            },
        },
        "resources": [
            # TODO: Add engagements here
        ],
    }

    for object_name, object_config in CRM_OBJECTS.items():
        # TODO: Update config
        api_config["resources"].append(None)

    return rest_api_source(api_config)
