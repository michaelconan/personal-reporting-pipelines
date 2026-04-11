"""
hubspot.py

Pipeline to load Hubspot CRM data into BigQuery.

API Resources:

- `Schemas <https://developers.hubspot.com/docs/reference/api/crm/objects/schemas>`_
- CRM Objects:
    - `Companies <https://developers.hubspot.com/docs/api-reference/crm-companies-v3/guide>`_
    - `Contacts <https://developers.hubspot.com/docs/api-reference/crm-contacts-v3/guide>`_
    - `Deals <https://developers.hubspot.com/docs/api-reference/crm-deals-v3/guide>`_
    - `Tickets <https://developers.hubspot.com/docs/api-reference/crm-tickets-v3/guide>`_
- CRM Engagements:
    - `Calls <https://developers.hubspot.com/docs/api-reference/crm-calls-v3/guide>`_
    - `Meetings <https://developers.hubspot.com/docs/api-reference/crm-meetings-v3/guide>`_
    - `Tasks <https://developers.hubspot.com/docs/api-reference/crm-tasks-v3/guide>`_
    - `Notes <https://developers.hubspot.com/docs/api-reference/crm-notes-v3/guide>`_
    - `Communications <https://developers.hubspot.com/docs/api-reference/crm-communications-v3/guide>`_  # noqa: E501
- `Associations <https://developers.hubspot.com/docs/api-reference/crm-associations-v4/guide>`_
"""

# Base
from logging import getLogger, Logger
from typing import Generator

# PyPI
import yaml
import pendulum
import requests

# dlt
import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.helpers.rest_client.paginators import (
    JSONResponseCursorPaginator,
)
from dlt.sources import DltResource
from dlt.common.pipeline import LoadInfo

# Common custom tasks
from pipelines import RAW_SCHEMA, BASE_DATE, SECRET_STORE
from pipelines.common.utils import (
    get_refresh_mode,
    get_write_disposition,
    log_refresh_mode,
    validate_required_secrets,
)


logger: Logger = getLogger(__name__)


def iso_to_unix(iso_date: str) -> int:
    """Convert ISO date string to Unix timestamp in milliseconds.

    Args:
        iso_date (str): ISO format date string (e.g., '2023-01-15T10:30:00').

    Returns:
        int: Unix timestamp in milliseconds.
    """
    dt = pendulum.parse(iso_date)
    return int(dt.timestamp() * 1000)


def map_engagement(item: dict) -> dict:
    """Transform engagement data by converting timestamps to ISO format.

    Args:
        item (dict): Raw engagement item from HubSpot API.

    Returns:
        dict: Transformed engagement item with ISO formatted timestamps.
    """
    item["engagement"]["lastUpdated"] = pendulum.from_timestamp(
        item["engagement"]["lastUpdated"] / 1000,
        tz="UTC",
    ).isoformat()
    return item


@dlt.source
def hubspot_source(
    session: requests.Session | None = None,
    initial_date: str = BASE_DATE,
    end_date: str | None = None,
) -> Generator[DltResource, None, None]:
    """Create a DLT source for HubSpot CRM data.

    This function configures and returns a DLT source for extracting contacts,
    companies, engagements, and schema data from the HubSpot CRM API.

    Args:
        session (requests.Session, optional): Optional requests session for
            HTTP calls. Defaults to None.
        initial_date (str, optional): Start date for data extraction in
            YYYY-MM-DD format. Defaults to `BASE_DATE`.
        end_date (str, optional): Optional end date for data extraction in
            YYYY-MM-DD format. Defaults to None.

    Yields:
        Generator[DltResource, None, None]: DLT resources configured for HubSpot
            data extraction.
    """
    api_key = dlt.secrets["sources.hubspot.api_key"]

    with open("pipelines/hs_config.yml", "rb") as fp:
        hs_config = yaml.safe_load(fp)
    crm_objects = hs_config["objects"]

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
    if session:
        api_config["client"]["session"] = session

    # Add schema resources
    for hs_object in crm_objects:
        object_name = hs_object["name"]
        schema_resource = {
            "name": f"hubspot__schemas_{object_name}",
            "max_table_nesting": 1,
            "columns": {
                "required_properties": {"data_type": "json"},
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

    # Add CRM and engagement object endpoint resources
    for hs_object in crm_objects:
        resource_name = f"hubspot__{hs_object['name']}"
        object_resource = {
            "name": resource_name,
            "endpoint": {
                "path": f"crm/v3/objects/{hs_object['name']}/search",
                "method": "POST",
                "data_selector": "results",
                "json": {
                    "limit": 100,
                    "properties": hs_object["properties"],
                    "filterGroups": [
                        {
                            "filters": [
                                {
                                    "propertyName": hs_object["filter"],
                                    "operator": "GTE",
                                    "value": "{incremental.start_value}",
                                },
                            ],
                        },
                    ],
                },
                "paginator": JSONResponseCursorPaginator(
                    cursor_path="paging.next.after",
                    cursor_body_path="after",
                ),
                "incremental": {
                    "cursor_path": "updatedAt",
                    "initial_value": initial_date,
                    "end_value": end_date,
                    "convert": iso_to_unix,
                },
            },
        }

        if end_date:
            object_resource["endpoint"]["json"]["filterGroups"][0]["filters"].append(
                {
                    "propertyName": hs_object["filter"],
                    "operator": "LTE",
                    "value": "{incremental.end_value}",
                },
            )

        api_config["resources"].append(object_resource)

        for hs_association in hs_object.get("associations", []):
            resource_path = (
                f"crm/v4/objects/{hs_object['name']}/"
                "{resources." + resource_name + ".id}"
                f"/associations/{hs_association}"
            )
            api_config["resources"].append(
                {
                    "name": f"{resource_name}_to_{hs_association}",
                    "max_table_nesting": 1,
                    "columns": {
                        "association_types": {"data_type": "json"},
                    },
                    "endpoint": {
                        "path": resource_path,
                        "method": "GET",
                        "data_selector": "results",
                    },
                    "include_from_parent": ["id", "updatedAt"],
                },
            )

    yield from rest_api_resources(api_config)


def refresh_hubspot(
    is_incremental: bool | None = None,
    pipeline: dlt.Pipeline | None = None,
    initial_date: str = BASE_DATE,
    end_date: str | None = None,
) -> LoadInfo:
    """Refresh HubSpot CRM data pipeline.

    Args:
        is_incremental (bool, optional): Override incremental mode.
            If None, uses environment-based detection. Defaults to None.
        pipeline (dlt.Pipeline, optional): dlt pipeline object.
            If None, a new one is created. Defaults to None.
        initial_date (str, optional): The start date for the data extraction.
            Defaults to `BASE_DATE`.
        end_date (str, optional): The end date for the data extraction.
            Defaults to None.

    Returns:
        dlt.common.pipeline.LoadInfo: Pipeline run information and status.
    """
    validate_required_secrets(
        secret_store=SECRET_STORE,
        required_secret_keys=["sources.hubspot.api_key"],
        pipeline_name="HubSpot CRM",
    )

    # Determine refresh mode if not explicitly provided
    if is_incremental is None:
        is_incremental = get_refresh_mode(default_incremental=True)

    # Log the refresh mode being used
    log_refresh_mode("HubSpot CRM", is_incremental, RAW_SCHEMA)

    # define the data load tool pipeline for tasks
    pipeline_name = "hubspot_crm_pipeline"

    # create hubspot crm dlt source
    hs_source = hubspot_source(initial_date=initial_date, end_date=end_date)

    if not pipeline:
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
    logger.info(info)
    return info


if __name__ == "__main__":
    info = refresh_hubspot()
