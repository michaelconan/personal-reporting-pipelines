"""
Common utilities for pipeline operations.
"""

# Base imports
import json
import os
import subprocess  # nosec B404
import tempfile
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


def update_onepassword_item(
    item_name: str,
    vault: str,
    field_updates: dict[str, str],
) -> None:
    """Update fields on a 1Password item using the JSON template flow.

    Exports the item as JSON, updates the specified fields by label, writes
    the result to a temp file, and calls ``op item edit --template``. Any
    field value that appears in error output is masked so tokens are not
    leaked in logs.

    Args:
        item_name: Name (or ID) of the 1Password item to update.
        vault: Vault containing the item.
        field_updates: Mapping of field label to new value.

    Raises:
        RuntimeError: If any ``op`` subprocess call fails.
    """
    sensitive_values = list(field_updates.values())

    def _mask(text: str) -> str:
        for val in sensitive_values:
            text = text.replace(val, "***")
        return text

    def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, check=True, stdin=subprocess.DEVNULL, **kwargs)  # nosec B603
        except subprocess.CalledProcessError as e:
            masked_cmd = [_mask(part) for part in e.cmd]
            raise RuntimeError(
                f"op command failed (exit code {e.returncode}): "
                f"cmd={masked_cmd}, stderr={_mask(e.stderr)}"
            ) from None

    # Export existing item as JSON
    export = _run(["op", "item", "get", item_name, "--vault", vault, "--format", "json"])  # nosec B607
    item = json.loads(export.stdout)

    # Update fields by label
    for field in item.get("fields", []):
        label = field.get("label")
        if label in field_updates:
            field["value"] = field_updates[label]

    # Write template and apply update
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=True) as tmp:
        json.dump(item, tmp)
        tmp.flush()
        _run(["op", "item", "edit", item_name, "--vault", vault, f"--template={tmp.name}"])  # nosec B607

    logger.info("Updated 1Password item '%s' fields: %s", item_name, list(field_updates.keys()))


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
