"""
hubspot.py

Pipeline to load Hubspot CRM data into BigQuery.

API Resources:

- `Contacts <https://developers.hubspot.com/docs/reference/api/crm/objects/contacts>`_
- `Companies <https://developers.hubspot.com/docs/reference/api/crm/objects/companies>`_
- `Engagements <https://developers.hubspot.com/docs/reference/api/crm/engagements/engagement-details>`_
- `Schemas <https://developers.hubspot.com/docs/reference/api/crm/objects/schemas>`_
"""

# Base
from logging import getLogger, Logger
from typing import Optional

# PyPI
import pendulum

# dlt
import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.helpers.rest_client.paginators import (
    JSONResponseCursorPaginator,
    OffsetPaginator,
)

# Common custom tasks
from pipelines import RAW_SCHEMA, IS_TEST
from pipelines.common.utils import (
    get_refresh_mode,
    get_write_disposition,
    log_refresh_mode,
)


logger: Logger = getLogger(__name__)


def iso_to_unix(iso_date: str) -> int:
    """
    Convert ISO date string to Unix timestamp in milliseconds.
    """
    dt = pendulum.parse(iso_date)
    return int(dt.timestamp() * 1000)


def map_engagement(item: dict) -> dict:
    item["engagement"]["lastUpdated"] = pendulum.from_timestamp(
        item["engagement"]["lastUpdated"] / 1000,
        tz="UTC",
    ).isoformat()
    return item


@dlt.source
def hubspot_source(
    api_key: str = dlt.secrets.value,
    initial_date: str = "2024-01-01",
    is_incremental: bool = True,
):

    # Set default incremental dates
    initial_dt = pendulum.parse(initial_date)
    if IS_TEST:
        initial_date = pendulum.date(2025, 1, 1)
        end_dt = initial_dt.add(days=90)
    else:
        end_dt = pendulum.now("UTC")

    CRM_OBJECTS = {
        "contacts": {
            "type": "object",
            "filter_key": "lastmodifieddate",
            "properties": [
                "email",
                "associatedcompanyid",
                "firstname",
                "lastname",
                "updatedAt",
            ],
        },
        "companies": {
            "type": "object",
            "filter_key": "hs_lastmodifieddate",
            "properties": [
                "name",
                "hs_ideal_customer_profile",
                "updatedAt",
            ],
        },
        "engagements": {
            "type": "activity",
            # Exclude: TASK, NOTE
            "types": [
                "CALL",
                "WHATS_APP",
                "MEETING",
                "SMS",
                "EMAIL",
                "LINKEDIN_MESSAGE",
            ],
            "properties": [
                "lastUpdated",
                "type",
                "timestamp",
                "bodyPreview",
                "contactIds",
                "companyIds",
            ],
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
        },
        "resources": [],
    }

    # Add schema resources only during full refresh
    if not is_incremental:
        for object_name in ["contacts", "companies"]:
            schema_resource = {
                "name": f"hubspot__schemas_{object_name}",
                "max_table_nesting": 1,
                "columns": {
                    "searchable_properties": {"data_type": "json"},
                    "secondary_display_properties": {"data_type": "json"},
                },
                "table_name": "hubspot__schemas",
                "endpoint": {
                    "path": f"crm-object-schemas/v3/schemas/{object_name}",
                    "method": "GET",
                    "data_selector": "$",
                },
                "write_disposition": "merge",
                "primary_key": "id",
            }
            api_config["resources"].append(schema_resource)

    # Add engagements resource
    # For incremental: use modified endpoint (30-day limitation)
    # For full refresh: use paged endpoint with date range (no 30-day limitation)
    if is_incremental:
        # Incremental: use modified engagements endpoint (30-day limitation)
        days_ago = pendulum.now("UTC") - initial_dt
        if days_ago < pendulum.duration(days=30):
            engagement_resource = {
                "name": "hubspot__engagements",
                "max_table_nesting": 1,
                "processing_steps": [{"map": map_engagement}],
                "endpoint": {
                    "path": "engagements/v1/engagements/recent/modified",
                    "method": "GET",
                    "data_selector": "results",
                    "params": {
                        "since": "{incremental.start_value}",
                    },
                    "paginator": OffsetPaginator(
                        offset_param="offset",
                        limit_param="limit",
                        total_path=None,
                        stop_after_empty_page=True,
                        limit=250,
                        has_more_path="hasMore",
                    ),
                },
            }
        else:
            # Skip engagements for incremental beyond 30 days
            engagement_resource = None
            logger.warning(
                "Skipping engagements for incremental load beyond 30 days "
                "due to HubSpot API limitations on recent/modified endpoint"
            )
    else:
        # Full refresh: use paged endpoint with date range (no 30-day limitation)
        engagement_resource = {
            "name": "hubspot__engagements",
            "max_table_nesting": 1,
            "processing_steps": [{"map": map_engagement}],
            "endpoint": {
                "path": "engagements/v1/engagements/paged",
                "method": "GET",
                "data_selector": "results",
                "incremental": {
                    "cursor_path": "engagement.lastUpdated",
                    "initial_value": initial_date,
                    # "end_value": end_dt.isoformat(),
                    "convert": iso_to_unix,
                },
                "paginator": OffsetPaginator(
                    offset_param="offset",
                    limit_param="limit",
                    total_path=None,
                    stop_after_empty_page=True,
                    limit=250,
                    has_more_path="hasMore",
                ),
            },
        }

    if engagement_resource:
        api_config["resources"].append(engagement_resource)

    # Add CRM object endpoint resources
    for object_name, object_config in CRM_OBJECTS.items():
        if object_config["type"] == "object":
            # Generate resource configuration for each CRM object
            object_resource = {
                "name": f"hubspot__{object_name}",
                "endpoint": {
                    "path": f"crm/v3/objects/{object_name}/search",
                    "method": "POST",
                    "data_selector": "results",
                    "paginator": JSONResponseCursorPaginator(
                        cursor_path="paging.next.after",
                        cursor_body_path="after",
                    ),
                },
            }

            # Define base JSON body for search
            body = {
                "limit": 100,
                "properties": object_config["properties"],
            }

            # Add incremental configuration if incremental loading is enabled
            if is_incremental:
                object_resource["endpoint"]["incremental"] = {
                    "cursor_path": "updatedAt",
                    "initial_value": initial_date,
                    # "end_value": end_dt.isoformat(),
                    "convert": iso_to_unix,
                }
                # Add search filters for incremental load
                body["filterGroups"] = [
                    {
                        "propertyName": object_config["filter_key"],
                        "operator": "GTE",
                        "value": "{incremental.start_value}",
                    },
                    # {
                    #     "propertyName": object_config["filter_key"],
                    #     "operator": "LTE",
                    #     "value": "{incremental.end_value}",
                    # },
                ]

            object_resource["endpoint"]["json"] = body
            api_config["resources"].append(object_resource)

    yield from rest_api_resources(api_config)


def refresh_hubspot(
    is_incremental: Optional[bool] = None,
):
    """
    Refresh HubSpot CRM data pipeline.

    Args:
        is_incremental: Override incremental mode. If None, uses environment-based detection.
    """

    # Determine refresh mode if not explicitly provided
    if is_incremental is None:
        is_incremental = get_refresh_mode(default_incremental=True)

    # Log the refresh mode being used
    log_refresh_mode("HubSpot CRM", is_incremental, RAW_SCHEMA)

    # define the data load tool pipeline for tasks
    pipeline_name = "hubspot_crm_pipeline"

    # create hubspot crm dlt source
    hs_source = hubspot_source(
        is_incremental=is_incremental,
    )

    # Modify the pipeline parameters
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        # TODO: Sort out how to define schema using params
        dataset_name=RAW_SCHEMA,
        destination="bigquery",
    )

    # Get appropriate write disposition
    write_disposition = get_write_disposition(is_incremental)

    # serialise the pipeline into airflow tasks
    info = pipeline.run(
        hs_source,
        write_disposition=write_disposition,
    )

    return info


if __name__ == "__main__":
    info = refresh_hubspot()
    logger.info(info)
