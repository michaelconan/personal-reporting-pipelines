"""
raw_notion__habits.py

DAGs to load daily and weekly habits from Notion API into BigQuery.
"""

import pendulum

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.destinations import bigquery
from dlt.helpers.airflow_helper import PipelineTasksGroup
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

# Airflow hooks and operators
from airflow.models import DAG
from airflow.hooks.base import BaseHook

# Common custom tasks
from dags.michael import DEFAULT_ARGS, RAW_SCHEMA, IS_TEST
from dags.michael.datasets import NOTION_DS
from dags.michael.common.utils import filter_fields

# Connection IDs
NOTION_CONN_ID = "notion_productivity"
BQ_CONN_ID = "bigquery_reporting"

# list[str]: JSONPath expressions to exclude attributes from the Notion source records
EXCLUDE_PATHS = [
    # Exclude standard database metadata fields (object, type)
    # e.g., parent__type ("database_id"), created_by__object ("user")
    # and database property metadata fields (id, type)
    # e.g., properties__name__type ("title"), properties__date__id ("abZq")
    # This does not exclude relation property IDs (e.g., "$.properties.relation[*].id")
    "$..object",
    "$..type",
    "$.properties.*.id",
    # Exclude database property list paginator flags (boolean)
    # If longer lists were expected, would need transformer for additional API calls
    "$.properties.*.has_more",
    # Exclude database property annotations (rich text)
    "$.properties..annotations",
]


def get_notion_source(
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

    api_config = {
        "client": {
            "base_url": "https://api.notion.com/v1",
            "auth": {
                "type": "bearer",
                "token": api_key,
            },
            "headers": {
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            "paginator": JSONResponseCursorPaginator(
                cursor_path="next_cursor",
                cursor_body_path="start_cursor",
            ),
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {
                "method": "POST",
            },
        },
        "resources": [
            {
                "name": "notion__databases",
                "max_table_nesting": 1,
                "columns": {"title": {"data_type": "json"}},
                "processing_steps": [
                    # Exclude database property metadata details entirely
                    {
                        "map": lambda r: filter_fields(
                            r,
                            EXCLUDE_PATHS + ["$.properties"],
                        )
                    },
                ],
                "endpoint": {
                    "path": "search",
                    "data_selector": "results",
                    "json": {
                        "query": db_name,
                        "filter": {
                            "property": "object",
                            "value": "database",
                        },
                    },
                },
            },
        ],
    }

    rows_resource = {
        "name": "notion__database_rows",
        # Add dynamic table name for the database rows resource
        "table_name": lambda r: f"notion__database_{r['parent']['database_id']}",
        # Prevent nested tables for multi-value properties
        "max_table_nesting": 2,
        "processing_steps": [
            {"map": lambda r: filter_fields(r, EXCLUDE_PATHS)},
        ],
        "endpoint": {
            "path": "databases/{resources.notion__databases.id}/query",
            "data_selector": "results",
            "json": {
                "filter": {
                    "and": [
                        {
                            "property": "Last edited time",
                            "date": {"after": "{incremental.start_value}"},
                        },
                        {
                            "property": "Last edited time",
                            "date": {"before": "{incremental.end_value}"},
                        },
                    ]
                },
            },
            "incremental": {
                "cursor_path": "last_edited_time",
                "initial_value": initial_date,
                "end_value": end_dt.isoformat(),
            },
        },
    }
    # Use airflow data interval for incremental runs
    if is_incremental:
        rows_resource["endpoint"]["incremental"]["allow_external_schedulers"] = True

    api_config["resources"].append(rows_resource)

    return rest_api_source(api_config)


DAG_CONFIGS = [
    {
        "dag_id": "raw_notion__habits__full",
        "schedule": "@once",
    },
    {
        "dag_id": "raw_notion__habits__changed",
        "schedule": "@weekly",
    },
]


def create_notion_dag(
    dag_id: str,
    schedule: str,
) -> DAG:
    with DAG(
        dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2024, 10, 1),
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["notion", "habits", "raw"],
    ) as dag:

        # check if the schedule is incremental
        is_incremental = schedule != "@once" and schedule is not None

        # define the data load tool pipeline for tasks
        pipeline_name = "notion_habits_pipeline"
        tasks = PipelineTasksGroup(
            pipeline_name, use_data_folder=False, wipe_local_data=True
        )

        # create notion databases dlt source
        notion_source = get_notion_source(
            db_name="Disciplines",
            api_key=BaseHook.get_connection(NOTION_CONN_ID).password,
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
            notion_source,
            decompose="serialize",
            trigger_rule="all_done",
            write_disposition=write_disposition,
            retries=0,
            outlets=[NOTION_DS],
        )

        return dag


# create DAGs dynamically
for config in DAG_CONFIGS:
    globals()[config["dag_id"]] = create_notion_dag(**config)
