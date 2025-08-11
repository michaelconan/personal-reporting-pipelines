"""
notion.py

Pipeline to load databases from Notion API into BigQuery.

API Resources:

- `Search Pages <https://developers.notion.com/reference/post-search>`_
- `Query Database <https://developers.notion.com/reference/post-database-query>`_
"""

from logging import getLogger, Logger
import pendulum
from typing import Optional

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

# Common custom tasks
from pipelines import RAW_SCHEMA, IS_TEST
from pipelines.common.utils import (
    get_refresh_mode,
    get_write_disposition,
    log_refresh_mode,
    filter_fields,
)

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

DATABASE_MAP = {
    "10140b50_0f0d_43d2_905a_7ed714ef7f2c": "daily_habits",
    "11e09eb8_3f76_80e7_8fac_e8d0bb538fb0": "weekly_habits",
    "18f09eb8_3f76_805c_a567_dde667374441": "monthly_habits",
}


logger: Logger = getLogger(__name__)


@dlt.source
def notion_source(
    db_name: str,
    api_key: str = dlt.secrets.value,
    initial_date: str = "2024-01-01",
    is_incremental: bool = True,
):

    # Set default incremental dates
    if IS_TEST:
        initial_date = "2025-01-01"
    initial_dt = pendulum.parse(initial_date)
    if IS_TEST:
        end_dt = initial_dt.add(days=90)
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
        "table_name": lambda r: f"notion__database_{DATABASE_MAP[r['parent']['database_id']]}",
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

    api_config["resources"].append(rows_resource)

    yield from rest_api_resources(api_config)


def refresh_notion(
    is_incremental: Optional[bool] = None,
):
    """
    Refresh Notion habits data pipeline.

    Args:
        is_incremental: Override incremental mode. If None, uses environment-based detection.
    """

    # Determine refresh mode if not explicitly provided
    if is_incremental is None:
        is_incremental = get_refresh_mode(default_incremental=True)

    # Log the refresh mode being used
    log_refresh_mode("Notion Habits", is_incremental, RAW_SCHEMA)

    # create notion databases dlt source
    pipeline_name = "notion_habits_pipeline"
    nt_source = notion_source(
        db_name="Disciplines",
        is_incremental=is_incremental,
    )

    # Modify the pipeline parameters
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        # TODO: Sort out how to define schema using params
        dataset_name=RAW_SCHEMA,
        destination="bigquery",
        progress="log",
    )

    # Get appropriate write disposition
    write_disposition = get_write_disposition(is_incremental)

    # Run pipeline from source
    info = pipeline.run(
        nt_source,
        write_disposition=write_disposition,
        loader_file_format="jsonl",
    )

    return info


if __name__ == "__main__":
    info = refresh_notion()
    logger.info(info)
