"""
Common utilities for pipeline operations.
"""

# Base imports
import os
from typing import Any
from logging import getLogger

# PyPI imports
from jsonpath_ng import parse

logger = getLogger(__name__)


# str: Sentinel value used to mark fields for deletion
SENTINEL = "__TO_DELETE__"


def _remove_sentinel(obj: Any, sentinel: str) -> Any:
    """Recursively remove fields with a sentinel value from a dictionary or list.

    The root object should be an object or list of objects, otherwise the function
    will return the object as is. This function is useful for cleaning up data
    structures where certain fields are marked for removal using a sentinel value.

    Args:
        obj (Any): Object to process, can be a dictionary, list, or primitive type.
        sentinel (str): Dedicated sentinel value to identify fields for removal.

    Returns:
        Any: Updated object with sentinel fields removed.
    """
    if isinstance(obj, dict):
        return {
            k: _remove_sentinel(v, sentinel) for k, v in obj.items() if v != sentinel
        }
    elif isinstance(obj, list):
        return [_remove_sentinel(v, sentinel) for v in obj if v != sentinel]
    else:
        return obj


def filter_fields(item: dict, paths: list[str]) -> dict:
    """Remove specified fields from a dictionary using JSONPath expressions.

    Args:
        item (dict): Record dictionary to apply filters to.
        paths (list[str]): One or more JSONPath expressions to filter out fields.

    Returns:
        dict: Updated dictionary with specified fields removed.
    """
    for path in paths:
        path_expr = parse(path)
        # path_expr.update(item, SENTINEL)
        path_expr.filter(lambda x: True, item)
    # return _remove_sentinel(item, SENTINEL)
    return item


def should_force_full_refresh() -> bool:
    """
    Check if a full refresh should be forced based on environment variables.

    Returns:
        bool: True if full refresh should be forced, False otherwise
    """
    return os.getenv("FORCE_FULL_REFRESH", "").lower() in ("true", "1", "yes")


def get_refresh_mode(default_incremental: bool = True) -> bool:
    """
    Determine the refresh mode based on environment variables and defaults.

    Args:
        default_incremental: Default incremental mode if no env var is set

    Returns:
        bool: True for incremental, False for full refresh
    """
    if should_force_full_refresh():
        logger.info("Full refresh forced via FORCE_FULL_REFRESH environment variable")
        return False

    # Check for specific pipeline override
    pipeline_name = os.getenv("PIPELINE_NAME", "").upper()
    if pipeline_name:
        full_refresh_var = f"{pipeline_name}_FULL_REFRESH"
        if os.getenv(full_refresh_var, "").lower() in ("true", "1", "yes"):
            logger.info(
                f"Full refresh forced for {pipeline_name} via {full_refresh_var}"
            )
            return False

    return default_incremental


def get_write_disposition(is_incremental: bool) -> str | None:
    """
    Get the appropriate write disposition for dlt pipeline.

    Args:
        is_incremental: Whether this is an incremental load

    Returns:
        str | None: Write disposition string or None for incremental
    """
    return None if is_incremental else "replace"


def log_refresh_mode(pipeline_name: str, is_incremental: bool, schema: str) -> None:
    """
    Log the refresh mode being used.

    Args:
        pipeline_name: Name of the pipeline
        is_incremental: Whether this is an incremental load
        schema: Target schema name
    """
    mode = "incremental" if is_incremental else "full refresh"
    logger.info(f"Running {pipeline_name} in {mode} mode to schema: {schema}")
