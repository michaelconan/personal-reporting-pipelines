"""
raw_hubspot__crm.py

DAGs to load Hubspot CRM data into BigQuery.
"""

import pendulum

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.destinations import bigquery
from dlt.helpers.airflow_helper import PipelineTasksGroup
from dlt.sources.helpers.rest_client.paginators import (
    JSONResponseCursorPaginator,
)

# Airflow hooks and operators
from airflow.models import DAG
from airflow.hooks.base import BaseHook

# Common custom tasks
from dags.michael.common.dlt_rest import HasMorePaginator
from dags.michael import DEFAULT_ARGS, RAW_SCHEMA, IS_TEST
from dags.michael.datasets import HUBSPOT_DS

# Connection IDs
HUBSPOT_CONN_ID = "hubspot_app"
BQ_CONN_ID = "bigquery_reporting"


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


def get_hubspot_source(
    api_key: str,
    initial_date: str = "2024-01-01",
    is_incremental: bool = True,
):

    # Set default incremental dates
    # Should be overriden by Airflow data interval
    initial_dt = pendulum.parse(initial_date)
    if IS_TEST:
        end_dt = initial_dt.add(days=7)
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

    # NOTE: Endpoint only supports 30 days of history or 10K records
    # If incremental and beyond 30 days, engagements will not be loaded
    days_ago = pendulum.now("UTC") - initial_dt
    if not is_incremental or days_ago < pendulum.duration(days=30):
        # Define base HubSpot engagements resource
        engagement_resouce = {
            "name": "hubspot__engagements",
            "processing_steps": [{"map": map_engagement}],
            "endpoint": {
                "path": "engagements/v1/engagements/paged",
                "method": "GET",
                "data_selector": "results",
                "incremental": {
                    "cursor_path": "engagement.lastUpdated",
                    "initial_value": initial_date,
                    "end_value": end_dt.isoformat(),
                    "convert": iso_to_unix,
                },
                "paginator": HasMorePaginator(
                    offset_param="offset",
                    limit_param="limit",
                    total_path=None,
                    stop_after_empty_page=True,
                    limit=250,
                    has_more_path="has_more",
                ),
            },
        }
        # Update for incremental properties if incremental load
        if is_incremental:
            # If incremental, use modified engagements endpoint
            engagement_resouce["endpoint"][
                "path"
            ] = "engagements/v1/engagements/recent/modified"
            engagement_resouce["endpoint"]["params"] = {
                "since": "{incremental.start_value}",
            }

        # Add engagement resource to API config
        api_config["resources"].append(engagement_resouce)

    # Add CRM object endpoint resources
    for object_name, object_config in CRM_OBJECTS.items():
        # Only update schema during full load
        if not is_incremental:
            # Add schema resource for the HubSpot object
            schema_resource = {
                "name": f"hubspot__schemas_{object_name}",
                "table_name": "hubspot__schemas",
                "endpoint": {
                    "path": f"crm-object-schemas/v3/schemas/{object_name}",
                    "method": "GET",
                    "data_selector": "$",
                },
                # Must merge as multiple objects are loaded to same schema tables
                # Unless we can do partition overwrite, TBD
                "write_disposition": "merge",
                "primary_key": "id",
            }
            api_config["resources"].append(schema_resource)

        if object_config["type"] == "object":
            # Generate resource configuration for each CRM object
            object_resource = {
                "name": f"hubspot__{object_name}",
                "endpoint": {
                    "path": f"crm/v3/objects/{object_name}/search",
                    "method": "POST",
                    "data_selector": "results",
                    "incremental": {
                        "cursor_path": "updatedAt",
                        "initial_value": initial_date,
                        "end_value": end_dt.isoformat(),
                        "convert": iso_to_unix,
                    },
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
            # Add search filters if incremental load
            if is_incremental:
                body["filterGroups"] = [
                    {
                        "propertyName": object_config["filter_key"],
                        "operator": "GTE",
                        "value": "{incremental.start_value}",
                    },
                    {
                        "propertyName": object_config["filter_key"],
                        "operator": "LTE",
                        "value": "{incremental.end_value}",
                    },
                ]
            object_resource["endpoint"]["json"] = body

            # Add object resource to API config
            api_config["resources"].append(object_resource)

    return rest_api_source(api_config)


DAG_CONFIGS = [
    {
        "dag_id": "raw_hubspot__crm__full",
        "schedule": "@once",
    },
    {
        "dag_id": "raw_hubspot__crm__changed",
        "schedule": "@weekly",
    },
]


def create_hubspot_dag(
    dag_id: str,
    schedule: str,
) -> DAG:
    with DAG(
        dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2024, 10, 1),
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["hubspot", "crm", "raw"],
    ) as dag:

        # check if the schedule is incremental
        is_incremental = schedule != "@once" and schedule is not None

        # define the data load tool pipeline for tasks
        pipeline_name = "hubspot_crm_pipeline"
        tasks = PipelineTasksGroup(
            pipeline_name, use_data_folder=False, wipe_local_data=True
        )

        # create hubspot crm dlt source
        hubspot_source = get_hubspot_source(
            api_key=BaseHook.get_connection(HUBSPOT_CONN_ID).password,
            is_incremental=is_incremental,
        )

        # create bigquery dlt destination
        bq_conn = BaseHook.get_connection(BQ_CONN_ID)
        bq_destination = bigquery(
            credentials=bq_conn.extra_dejson.get("keyfile_dict"),
        )

        # Modify the pipeline parameters
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            # TODO: Sort out how to define schema using params
            dataset_name=RAW_SCHEMA,
            destination=bq_destination,
            dev_mode=False,  # Must be false if we decompose
        )

        # serialise the pipeline into airflow tasks
        write_disposition = None if is_incremental else "replace"
        tasks.add_run(
            pipeline,
            hubspot_source,
            decompose="serialize",
            trigger_rule="all_done",
            write_disposition=write_disposition,
            retries=0,
            outlets=[HUBSPOT_DS],
        )

        return dag


# create DAGs dynamically
for config in DAG_CONFIGS:
    globals()[config["dag_id"]] = create_hubspot_dag(**config)
