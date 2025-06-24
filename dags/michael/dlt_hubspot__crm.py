import pendulum

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.destinations import bigquery
from dlt.helpers.airflow_helper import PipelineTasksGroup
from dlt.sources.helpers.rest_client.paginators import (
    JSONResponseCursorPaginator,
    OffsetPaginator,
)

# Airflow hooks and operators
from airflow.models import DAG
from airflow.hooks.base import BaseHook

# Common custom tasks
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


def get_search_filters(context: dict, object_info: dict, is_incremental: bool) -> dict:
    """
    Generate search filters for HubSpot API based on context.
    """
    body = {
        "limit": 100,
        "properties": object_info["properties"],
    }
    if is_incremental:
        body["filterGroups"] = [
            {
                "propertyName": object_info["filter_key"],
                "operator": "GTE",
                "value": iso_to_unix(context["incremental"]["start_value"]),
            },
            {
                "propertyName": object_info["filter_key"],
                "operator": "LTE",
                "value": iso_to_unix(context["incremental"]["end_value"]),
            },
        ]

    return body


def get_engagement_endpoint(
    context: dict, properties: list, is_incremental: bool
) -> dict:

    endpoint = (
        {
            "path": "engagements/v1/engagements/paged",
            "method": "GET",
            "data_selector": "results",
            "json": {
                "properties": properties,
            },
        },
    )
    if is_incremental:
        # NOTE: Endpoint only supports 30 days of history or 10K records
        start = pendulum.parse(context["incremental"]["start_value"])
        if pendulum.now("UTC") - start < pendulum.duration(days=30):
            endpoint["path"] = "/engagements/v1/engagements/recent/modified"
            endpoint["params"] = {
                "since": iso_to_unix(start.isoformat()),
            }
            endpoint["paginator"] = OffsetPaginator(
                offset_param="offset",
                limit_param="limit",
                limit=250,
            )

    return endpoint


def get_hubspot_source(
    db_name: str,
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
            "filter_key": "hs_lastmodifieddate",
            "properties": [
                "name",
                "hs_ideal_customer_profile",
                "updatedAt",
            ],
        },
        "engagements": {
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
            # Placeholders for incremental loading
            "endpoint": {
                "initial_value": initial_date,
                "end_value": end_dt.isoformat(),
            },
        },
        "resources": [
            {
                "name": "hubspot__engagements",
                "endpoint": lambda ctx: get_engagement_endpoint(
                    context=ctx,
                    properties=CRM_OBJECTS["engagements"]["properties"],
                    is_incremental=is_incremental,
                ),
            },
        ],
    }

    for object_name, object_config in CRM_OBJECTS.items():
        # Generate resource configuration for each CRM object
        api_config["resources"].append(
            {
                "name": f"hubspot__{object_name}",
                "endpoint": {
                    "path": f"crm/v3/objects/{object_name}/search",
                    "method": "POST",
                    "data_selector": "results",
                    "paginator": JSONResponseCursorPaginator(
                        cursor_path="paging.next.after",
                        cursor_body_path="after",
                    ),
                    "json": lambda ctx: get_search_filters(
                        context=ctx,
                        object_info=object_config,
                        is_incremental=is_incremental,
                    ),
                },
            }
        )

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
            db_name="Disciplines",
            api_key=BaseHook.get_connection(HUBSPOT_CONN_ID).password,
            is_incremental=is_incremental,
        )

        # create bigquery dlt destination
        bq_conn = BaseHook.get_connection(BQ_CONN_ID)
        bq_destination = bigquery(
            credentials=bq_conn.extra_dejson.get("keyfile_dict"),
        )

        # Limit the number of rows for testing purposes
        # if IS_TEST:
        #     notion_source.add_limit(100)

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
