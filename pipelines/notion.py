"""
notion.py

Pipeline to load data sources from Notion API into BigQuery.

API Resources:

- `Search Pages <https://developers.notion.com/reference/post-search>`_
- `Query Data Source <https://developers.notion.com/reference/query-a-data-source>`_
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
from pipelines import RAW_SCHEMA, BASE_DATE, SECRET_STORE
from pipelines.common.utils import (
    get_refresh_mode,
    get_write_disposition,
    log_refresh_mode,
    filter_fields,
    validate_required_secrets,
)

# list[str]: JSONPath expressions to exclude attributes from the Notion source records
EXCLUDE_PATHS = [
    # Exclude standard data source metadata fields (object, type)
    # e.g., parent__type ("data_source_id"), created_by__object ("user")
    # and data source property metadata fields (id, type)
    # e.g., properties__name__type ("title"), properties__date__id ("abZq")
    # This does not exclude relation property IDs (e.g., "$.properties.relation[*].id")
    "$..object",
    "$..type",
    "$.properties.*.id",
    # Exclude data source property list paginator flags (boolean)
    # If longer lists were expected, would need transformer for additional API calls
    "$.properties.*.has_more",
    # Exclude data source property annotations (rich text)
    "$.properties..annotations",
]

DATA_SOURCE_MAP = {
    "17b04335-b725-4511-ae86-594ef725706c": "daily_habits",
    "3042e681-b85e-494e-b287-9aa80827cd81": "weekly_habits",
    "18f09eb8-3f76-809f-8140-000bbccd5616": "monthly_habits",
    "2ec09eb8-3f76-80de-85f4-000b2cd39a1f": "habit_reference",
}


logger: Logger = getLogger(__name__)


def name_db_table(row: dict) -> str:
    """Generate a table name for Notion data source rows based on data source ID.

    Args:
        row (dict): A data source row containing parent data source information.

    Returns:
        str: Formatted table name using the data source mapping or ID.
    """
    db_id = row["parent"]["data_source_id"]
    suffix = DATA_SOURCE_MAP.get(db_id, db_id)
    return f"notion__data_source_{suffix}"


@dlt.source
def notion_source(
    db_name: str,
    initial_date: str | None = BASE_DATE,
    end_date: str | None = None,
    session: requests.Session | None = None,
) -> Generator[DltResource, None, None]:
    """Create a DLT source for Notion data source data.

    This function configures and returns a DLT source for extracting data source
    metadata and row data from the Notion API.

    Args:
        db_name (str): Name of the data source to search for and extract data from.
        initial_date (str, optional): Start date for data extraction in
            YYYY-MM-DD format. Defaults to `BASE_DATE`.
        end_date (str, optional): Optional end date for data extraction in
            YYYY-MM-DD format. Defaults to None.
        session (requests.Session, optional): Optional requests session for
            HTTP calls. Defaults to None.

    Yields:
        Generator[DltResource, None, None]: DLT resources configured for Notion
            data extraction.
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
                "Notion-Version": "2025-09-03",
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
                "name": "notion__data_sources",
                "max_table_nesting": 1,
                "columns": {
                    "title": {"data_type": "json"},
                    "description": {"data_type": "json"},
                },
                "processing_steps": [
                    # Exclude data source property metadata details entirely
                    {
                        "map": lambda r: filter_fields(
                            r,
                            EXCLUDE_PATHS + ["$.properties"],
                        ),
                    },
                ],
                "endpoint": {
                    "path": "search",
                    "data_selector": "results",
                    "json": {
                        "query": db_name,
                        "filter": {
                            "property": "object",
                            "value": "data_source",
                        },
                    },
                },
            },
        ],
    }

    rows_resource = {
        "name": "notion__data_source_rows",
        # Add dynamic table name for the data source rows resource
        "table_name": name_db_table,
        # Prevent nested tables for multi-value properties
        "max_table_nesting": 2,
        "columns": {"title": {"data_type": "json"}},
        "processing_steps": [
            {"map": lambda r: filter_fields(r, EXCLUDE_PATHS)},
        ],
        "endpoint": {
            "path": "data_sources/{resources.notion__data_sources.id}/query",
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
            ],
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
    """Refresh Notion habits data pipeline.

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
        required_secret_keys=["sources.notion.api_key"],
        pipeline_name="Notion Habits",
    )

    # Determine refresh mode if not explicitly provided
    if is_incremental is None:
        is_incremental = get_refresh_mode(default_incremental=True)

    # Log the refresh mode being used
    log_refresh_mode("Notion Habits", is_incremental, RAW_SCHEMA)

    # create notion data sources dlt source
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
