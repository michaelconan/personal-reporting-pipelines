"""
Common utilities for pipeline operations.
"""

# Base imports
import os
from logging import getLogger

# PyPI imports
import dlt
from jsonpath_ng import parse

logger = getLogger(__name__)


def validate_required_secrets(
    *,
    secret_store: str,
    required_secret_keys: list[str],
    pipeline_name: str,
) -> None:
    """Validate secret store configuration and required secrets.

    Args:
        secret_store: Backing secret store name
            (for example: google, 1password).
        required_secret_keys: DLT secret keys required by the pipeline.
        pipeline_name: Human-readable pipeline name for error messages.

    Raises:
        ValueError: If secret store value is unsupported.
        EnvironmentError: If store-specific configuration is missing.
    """
    supported_stores = {"google", "1password"}
    if secret_store not in supported_stores:
        raise ValueError(
            f"Unsupported SECRET_STORE '{secret_store}'. "
            f"Expected one of: {sorted(supported_stores)}.",
        )

    if secret_store == "google":
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path:
            raise EnvironmentError(
                "SECRET_STORE is 'google' but "
                "GOOGLE_APPLICATION_CREDENTIALS is not set.",
            )
    elif secret_store == "1password":
        op_token = os.getenv("OP_SERVICE_ACCOUNT_TOKEN")
        if not op_token:
            raise EnvironmentError(
                "SECRET_STORE is '1password' but "
                "OP_SERVICE_ACCOUNT_TOKEN is not set.",
            )

    missing_keys: list[str] = []
    for key in required_secret_keys:
        if not dlt.secrets.get(key):
            missing_keys.append(key)

    if missing_keys:
        missing = ", ".join(sorted(missing_keys))
        raise EnvironmentError(
            f"Missing required secrets for {pipeline_name} "
            f"(SECRET_STORE={secret_store}): {missing}",
        )


def filter_fields(item: dict, paths: list[str]) -> dict:
    """Remove specified fields from a dictionary using JSONPath expressions.

    Args:
        item (dict): Record dictionary to apply filters to.
        paths (list[str]): One or more JSONPath expressions to filter out
            fields.

    Returns:
        dict: Updated dictionary with specified fields removed.
    """
    for path in paths:
        path_expr = parse(path)
        path_expr.filter(lambda x: True, item)
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
        logger.info(
            "Full refresh forced via FORCE_FULL_REFRESH environment variable",
        )
        return False

    # Check for specific pipeline override
    pipeline_name = os.getenv("PIPELINE_NAME", "").upper()
    if pipeline_name:
        full_refresh_var = f"{pipeline_name}_FULL_REFRESH"
        if os.getenv(full_refresh_var, "").lower() in ("true", "1", "yes"):
            logger.info(
                f"Full refresh forced for {pipeline_name} "
                f"via {full_refresh_var}",
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


def log_refresh_mode(
    pipeline_name: str,
    is_incremental: bool,
    schema: str,
) -> None:
    """
    Log the refresh mode being used.

    Args:
        pipeline_name: Name of the pipeline
        is_incremental: Whether this is an incremental load
        schema: Target schema name
    """
    mode = "incremental" if is_incremental else "full refresh"
    logger.info(f"Running {pipeline_name} in {mode} mode to schema: {schema}")
