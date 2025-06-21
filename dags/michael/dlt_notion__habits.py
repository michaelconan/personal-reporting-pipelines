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
from dags.michael import RAW_SCHEMA, IS_TEST
from dags.michael.datasets import NOTION_DS

# Connection IDs
NOTION_CONN_ID = "notion_productivity"
BQ_CONN_ID = "bigquery_reporting"


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
                "name": "notion_databases",
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
        "name": "notion_database_rows",
        # Add dynamic table name for the database rows resource
        "table_name": lambda r: f"notion_database_{r['parent']['database_id']}",
        "endpoint": {
            "path": "databases/{resources.notion_databases.id}/query",
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


# Modify the DAG arguments
default_task_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": "test@test.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

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
        default_args=default_task_args,
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
