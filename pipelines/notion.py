"""
notion.py

Pipeline to load databases from Notion API into BigQuery.

API Resources:

- `Search Pages <https://developers.notion.com/reference/post-search>`_
- `Query Database <https://developers.notion.com/reference/post-database-query>`_
"""

# Baes imports
from logging import getLogger, Logger
from typing import Generator

# PyPI imports
import dlt
import requests
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator
from dlt.sources import DltResource
from dlt.common.pipeline import LoadInfo

# Common custom tasks
from pipelines import RAW_SCHEMA, BASE_DATE
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
    "10140b50-0f0d-43d2-905a-7ed714ef7f2c": "daily_habits",
    "11e09eb8-3f76-80e7-8fac-e8d0bb538fb0": "weekly_habits",
    "18f09eb8-3f76-805c-a567-dde667374441": "monthly_habits",
}


logger: Logger = getLogger(__name__)


def name_db_table(row: dict) -> str:
    """Generate a table name for Notion database rows based on database ID.

    Args:
        row: A database row containing parent database information.

    Returns:
        str: Formatted table name using the database mapping or ID.
    """
    db_id = row["parent"]["database_id"]
    suffix = DATABASE_MAP.get(db_id, db_id)
    return f"notion__database_{suffix}"


@dlt.source
def notion_source(
    db_name: str,
    initial_date: str | None = BASE_DATE,
    end_date: str | None = None,
    session: requests.Session | None = None,
) -> Generator[DltResource, None, None]:
    """Create a DLT source for Notion database data.

    This function configures and returns a DLT source for extracting database
    metadata and row data from the Notion API.

    Args:
        db_name: Name of the database to search for and extract data from.
        initial_date: Start date for data extraction in YYYY-MM-DD format.
        end_date: Optional end date for data extraction in YYYY-MM-DD format.
        session: Optional requests session for HTTP calls.

    Yields:
        DLT resources configured for Notion data extraction.
    """
    api_key = dlt.secrets["sources.notion.api_key"]

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
        "table_name": name_db_table,
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
                    "property": "Last edited time",
                    "date": {"after": "{incremental.start_value}"},
                },
            },
            "incremental": {
                "cursor_path": "last_edited_time",
                "initial_value": initial_date,
                "end_value": end_date,
            },
        },
    }

    if end_date:
        # Add end date to filters
        rows_resource["endpoint"]["json"]["filter"] = {
            "and": [
                rows_resource["endpoint"]["json"]["filter"],
                {
                    "property": "Last edited time",
                    "date": {"before": "{incremental.end_value}"},
                },
            ]
        }
        # Add end date to incremental load range
        # rows_resource["endpoint"]["incremental"]["end_value"] = end_date

    api_config["resources"].append(rows_resource)

    if session:
        api_config["client"]["session"] = session

    yield from rest_api_resources(api_config)


def refresh_notion(
    is_incremental: bool | None = None,
    pipeline: dlt.Pipeline | None = None,
    initial_date: str | None = BASE_DATE,
    end_date: str | None = None,
) -> LoadInfo:
    """
    Refresh Notion habits data pipeline.

    Args:
        is_incremental: Override incremental mode. If None, uses environment-based detection.
        pipeline: dlt pipeline object. If None, a new one is created.

    Returns:
        LoadInfo: Pipeline run information and status.
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
        initial_date=initial_date,
        end_date=end_date,
    )

    if not pipeline:
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
    logger.info(info)

    return info


if __name__ == "__main__":
    info = refresh_notion()
