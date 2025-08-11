"""
Common utilities for pipeline operations.
"""

import os
from typing import Optional
from logging import getLogger

logger = getLogger(__name__)


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


def get_write_disposition(is_incremental: bool) -> Optional[str]:
    """
    Get the appropriate write disposition for dlt pipeline.

    Args:
        is_incremental: Whether this is an incremental load

    Returns:
        str: Write disposition string or None for incremental
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
