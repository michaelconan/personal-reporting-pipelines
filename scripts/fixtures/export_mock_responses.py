# scripts/fixtures/export_mock_responses.py
"""
Script to capture, scrub, and structure raw API response bodies from dlt sources
into unit test mock data files under tests/fixtures.

For paginated/incremental resources (determined via dlt resource configuration):
  Output structures follow the 3-file pattern defined in tests/TESTS.md:
    - run1_page1: 3 records with the resource's pagination indicators
    - run1_page2: 2 records with pagination indicators removed/cleared
    - run2: 1 record with advanced timestamps/cursors for incremental testing

For non-paginated/non-incremental resources (e.g. schemas, metadata):
  Output is exported as a single scrubbed JSON file (<endpoint_name>.json).

Prerequisites
-------------
- API credentials for the respective sources configured in environment variables or dlt secrets.
- Sources with missing credentials are automatically skipped with a log warning.

Usage
-----
```bash
# Export mock responses for all configured sources
python scripts/fixtures/export_mock_responses.py

# Export mock responses for a specific source
python scripts/fixtures/export_mock_responses.py --source hubspot

# Perform a dry run without writing files to disk
python scripts/fixtures/export_mock_responses.py --dry-run
```
"""

import argparse
import copy
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import dlt
import requests
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from scripts.fixtures.scrub_data import resolve_project_root, scrub_api_response
except ImportError:  # direct script execution: expose the repo root, then retry
    sys.path.insert(0, str(PROJECT_ROOT))
    from scripts.fixtures.scrub_data import resolve_project_root, scrub_api_response

PROJECT_ROOT = resolve_project_root(Path(__file__).resolve().parent)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

MOCK_DATA_DIR = PROJECT_ROOT / "tests" / "fixtures"

# Registry of capturable sources. Adding a source means adding one entry here:
# no new runner function or CLI branch is required.
SOURCE_REGISTRY: dict[str, dict[str, Any]] = {
    "hubspot": {
        "secret_key": "sources.hubspot.api_key",
        "initial_date": "1970-01-01",
        "end_date": "2100-01-01",
        "page_size_property": "limit",
        "pipeline_name": "mock_export_hs",
        "display_name": "HubSpot",
        "source_kwargs": {
            "initial_date": "1970-01-01",
            "end_date": "2100-01-01",
        },
    },
    "notion": {
        "secret_key": "sources.notion.api_key",
        "initial_date": "1970-01-01",
        "end_date": "2100-01-01",
        "page_size_property": "page_size",
        "pipeline_name": "mock_export_notion",
        "display_name": "Notion",
        "database_name": "Disciplines",
        "source_kwargs": {
            "db_name": "Disciplines",
            "initial_date": "1970-01-01",
            "end_date": "2100-01-01",
        },
    },
    "google_health": {
        "secret_key": "sources.google_health.refresh_token",
        "page_size_property": "pageSize",
        "pipeline_name": "mock_export_gh",
        "display_name": "Google Health",
        "token_refresh": True,
    },
}

# Backwards-compatible aliases (referenced by tests and source factories).
HUBSPOT_EXPORT_INITIAL_DATE = SOURCE_REGISTRY["hubspot"]["initial_date"]
HUBSPOT_EXPORT_END_DATE = SOURCE_REGISTRY["hubspot"]["end_date"]
NOTION_EXPORT_INITIAL_DATE = SOURCE_REGISTRY["notion"]["initial_date"]
NOTION_EXPORT_END_DATE = SOURCE_REGISTRY["notion"]["end_date"]
NOTION_EXPORT_DATABASE_NAME = SOURCE_REGISTRY["notion"]["database_name"]


def get_secret(secret_path: str) -> str | None:
    """Resolve a dlt secret from either dlt config or the equivalent environment variable."""
    try:
        if hasattr(dlt, "secrets") and dlt.secrets is not None:
            value: Any = dlt.secrets.get(secret_path)
            if value:
                return value
    except Exception:
        logger.debug("Unable to resolve dlt secret", exc_info=True)

    env_name = secret_path.upper().replace(".", "__").replace("-", "_")
    return os.getenv(env_name) or os.getenv(env_name.replace("__", "_"))


class ResponseCaptureSession(requests.Session):
    """Capture raw JSON responses at the actual network boundary used by requests.

    dlt's REST client may call Session.send directly rather than only firing the
    response hook callbacks, so recording in send() is the reliable capture point.
    """

    def __init__(self) -> None:
        super().__init__()
        self.captured_responses: list[tuple[str, Any, dict[str, Any]]] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:
        """Call the parent implementation and record any successful JSON payloads."""
        response = super().send(request, **kwargs)
        if 200 <= response.status_code < 300:
            try:
                payload = response.json()
                self.captured_responses.append(
                    (response.url, payload, dict(getattr(response, "headers", {})))
                )
            except Exception:
                logger.debug("Unable to decode captured response as JSON", exc_info=True)
        return response


def get_resource_config_dicts(resource: Any) -> list[dict[str, Any]]:
    """Collect config dictionaries attached to a dlt resource's pipe steps."""
    configs: list[dict[str, Any]] = []
    try:
        pipe = getattr(resource, "_pipe", None)
        if not pipe:
            return configs

        for step in pipe.steps:
            if hasattr(step, "__closure__") and step.__closure__:
                for cell in step.__closure__:
                    value = getattr(cell, "cell_contents", None)
                    if isinstance(value, dict):
                        configs.append(value)
            for value in getattr(step, "__dict__", {}).values():
                if isinstance(value, dict):
                    configs.append(value)
    except Exception:
        logger.debug("Unable to inspect resource configuration", exc_info=True)
    return configs


def get_resource_pagination_metadata(resource: Any) -> dict[str, Any]:
    """Read dlt paginator/incremental metadata from the resource rather than hardcoded field names."""
    metadata: dict[str, Any] = {
        "is_paginated_or_incremental": False,
        "incremental_cursor_path": None,
        "incremental_cursor_transform": None,
        "paginator_cursor_path": None,
        "paginator_cursor_param": None,
        "paginator_cursor_body_path": None,
        "paginator_has_more_path": None,
    }

    has_incremental_config = False
    try:
        incremental = getattr(resource, "incremental", None)
        if incremental is not None:
            has_incremental_config = True
            metadata["is_paginated_or_incremental"] = True
            cursor_path = getattr(incremental, "cursor_path", None)
            if cursor_path:
                metadata["incremental_cursor_path"] = cursor_path
            metadata["incremental_cursor_transform"] = getattr(
                incremental, "cursor_transform", None
            )
    except Exception:
        logger.debug("Unable to inspect incremental configuration", exc_info=True)

    for config in get_resource_config_dicts(resource):
        paginator = config.get("paginator") or getattr(config.get("client"), "paginator", None)
        incremental_obj = config.get("incremental_object")
        if paginator is not None:
            for key in ("cursor_path", "cursor_param", "cursor_body_path", "has_more_path"):
                val = getattr(paginator, key, None)
                if val is not None:
                    metadata[f"paginator_{key}"] = str(val)

        if incremental_obj is not None:
            has_incremental_config = True
            cursor_path = getattr(incremental_obj, "cursor_path", None)
            if cursor_path:
                metadata["incremental_cursor_path"] = cursor_path
            metadata["incremental_cursor_transform"] = getattr(
                incremental_obj, "cursor_transform", None
            )

    metadata["is_paginated_or_incremental"] = bool(
        metadata["incremental_cursor_path"]
        or (metadata["paginator_cursor_path"] and has_incremental_config)
    )

    return metadata


def is_resource_paginated_or_incremental(resource: Any) -> bool:
    """Check dlt resource configuration to determine if it is paginated or incremental."""
    return get_resource_pagination_metadata(resource)["is_paginated_or_incremental"]


def collect_source_resource_configs(source: Any, sources_map: dict[str, dict[str, Any]]) -> None:
    """Populate sources_map dictionary with resource metadata derived from dlt config."""
    try:
        for res_name, resource in source.resources.items():
            metadata = get_resource_pagination_metadata(resource)
            metadata["resource_name"] = res_name
            metadata["endpoint_paths"] = []
            for config in get_resource_config_dicts(resource):
                path = config.get("path")
                if path:
                    metadata["endpoint_paths"].append(path)
            if not metadata["endpoint_paths"]:
                metadata["endpoint_paths"] = [res_name]
            sources_map[res_name] = metadata
    except Exception as e:
        logger.warning(f"Error inspecting source resource config: {e}")


def override_source_paginators(source: Any) -> None:
    """Use one API request per resource while exporting raw response fixtures."""

    def replace_paginators(value: Any) -> None:
        if isinstance(value, dict):
            if value.get("paginator") is not None:
                value["paginator"] = SinglePagePaginator()
            client = value.get("client")
            if client is not None and getattr(client, "paginator", None) is not None:
                client.paginator = SinglePagePaginator()
            for child in value.values():
                replace_paginators(child)
        elif isinstance(value, list):
            for child in value:
                replace_paginators(child)

    try:
        for resource in source.resources.values():
            for config in get_resource_config_dicts(resource):
                replace_paginators(config)
    except Exception as e:
        raise RuntimeError("Unable to override source paginators for mock export") from e


def override_incremental_resource_property(
    source: Any,
    property_name: str,
    value: int = 6,
    resource_values: dict[str, int] | None = None,
) -> None:
    """Override an endpoint request property on every incremental dlt resource.

    Args:
        source: dlt source whose resource configs are mutated in place.
        property_name: Request property to override (e.g. ``limit``,
            ``page_size``, ``pageSize``).
        value: Default override applied to all incremental resources.
        resource_values: Optional per-resource overrides keyed by resource
            name; resources absent from the mapping fall back to ``value``.
    """
    try:
        for res_name, resource in source.resources.items():
            metadata = get_resource_pagination_metadata(resource)
            if not metadata["incremental_cursor_path"]:
                continue
            override = (resource_values or {}).get(res_name, value)
            for config in get_resource_config_dicts(resource):
                for request_key in ("params", "json"):
                    request = config.get(request_key)
                    if isinstance(request, dict) and property_name in request:
                        request[property_name] = override
    except Exception as e:
        raise RuntimeError(
            f"Unable to override incremental resource property {property_name!r}"
        ) from e


def endpoint_name_from_resource_name(resource_name: str) -> str:
    """Return the dlt resource name used as the mock filename prefix."""
    return resource_name


def mock_system_name(endpoint_name: str) -> str:
    """Return the canonical mock-data directory for an exported endpoint."""
    return endpoint_name.split("__", 1)[0] if "__" in endpoint_name else "misc"


def _compile_endpoint_pattern(endpoint_path: str) -> "re.Pattern[str]":
    """Compile a dlt endpoint template (with ``{placeholders}``) to a regex."""
    path_parts = re.split(r"(\{[^}]+\})", endpoint_path)
    pattern = "".join(
        "[^/]+" if part.startswith("{") and part.endswith("}") else re.escape(part)
        for part in path_parts
        if part
    )
    return re.compile(rf"(^|/){pattern}($|/)")


def build_matchers(
    sources_map: dict[str, dict[str, Any]],
) -> list[tuple["re.Pattern[str]", int, dict[str, Any]]]:
    """Precompile endpoint matchers once per export instead of per response.

    Returns:
        List of (regex, specificity score, resource metadata) tuples.
    """
    matchers: list[tuple[re.Pattern[str], int, dict[str, Any]]] = []
    for meta in sources_map.values():
        for endpoint_path in meta.get("endpoint_paths", []):
            matchers.append((_compile_endpoint_pattern(endpoint_path), len(endpoint_path), meta))
    return matchers


def match_resource_for_url(
    url: str,
    sources_map: dict[str, dict[str, Any]],
    *,
    _matchers: list[tuple["re.Pattern[str]", int, dict[str, Any]]] | None = None,
) -> dict[str, Any] | None:
    """Match a response URL to a source resource using the dlt-configured endpoint path patterns."""
    url_path = url.split("?", 1)[0]
    matchers = _matchers if _matchers is not None else build_matchers(sources_map)
    best_match: dict[str, Any] | None = None
    best_score = -1
    for regex, score, meta in matchers:
        if regex.search(url_path) and score > best_score:
            best_match = meta
            best_score = score
    return best_match


# Response keys that hold metadata rather than records; excluded when
# dynamically detecting the record list in an unfamiliar payload.
_METADATA_FIELDS = {
    "paging",
    "token",
    "hasmore",
    "has_more",
    "total",
    "next_cursor",
    "nextcursor",
    "object",
}


def detect_data_field(payload: dict[str, Any]) -> str | None:
    """Detect the key containing record lists in an API response payload.

    Known dlt record keys are preferred; otherwise the first list-of-dicts
    key (excluding pagination metadata) is used, so new sources work without
    code changes.
    """
    for field in ["results", "sleep", "activities", "dataPoints"]:
        if field in payload and isinstance(payload[field], list):
            return field
    for field, value in payload.items():
        if not isinstance(value, list) or not value:
            continue
        if field.lower() in _METADATA_FIELDS:
            continue
        if all(isinstance(item, dict) for item in value):
            return field
    return None


def get_nested_value(data: Any, path: str | None) -> Any:
    """Return a nested value from a dlt-configured dotted path."""
    if not path:
        return None
    current = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        if part not in current:
            return None
        current = current[part]
    return current


def set_nested_value(data: Any, path: str | None, value: Any) -> None:
    """Set a value at a dlt-configured dotted path."""
    if not path:
        return
    parts = path.split(".")
    current = data
    for part in parts[:-1]:
        if not isinstance(current, dict):
            return
        current = current.setdefault(part, {})
    if isinstance(current, dict):
        current[parts[-1]] = value


def delete_nested_value(data: Any, path: str | None) -> None:
    """Delete a value at a dlt-configured dotted path."""
    if not path or not isinstance(data, dict):
        return
    parts = path.split(".")
    if len(parts) == 1:
        data.pop(parts[0], None)
        return

    child = data.get(parts[0])
    delete_nested_value(child, ".".join(parts[1:]))
    if isinstance(child, dict) and not child:
        data.pop(parts[0], None)


def update_pagination_fields(
    payload: dict[str, Any],
    *,
    endpoint_name: str,
    page_1: bool,
    paginator_cursor_path: str | None = None,
    paginator_has_more_path: str | None = None,
) -> None:
    """Validate or remove fields at paths configured by the dlt paginator."""
    if not paginator_cursor_path:
        return

    cursor_value = get_nested_value(payload, paginator_cursor_path)
    if page_1:
        if cursor_value is None:
            raise ValueError(
                f"Captured response is missing the dlt paginator cursor path "
                f"{paginator_cursor_path!r} for resource {endpoint_name!r}"
            )
        return

    delete_nested_value(payload, paginator_cursor_path)
    if paginator_has_more_path:
        set_nested_value(payload, paginator_has_more_path, False)


def pad_records(records: list[dict[str, Any]], target_count: int = 6) -> list[dict[str, Any]]:
    """Ensure records list has at least target_count items by deep-copying and updating IDs."""
    if not records:
        return []
    result = copy.deepcopy(records)
    original_len = len(records)
    while len(result) < target_count:
        idx = len(result)
        sample = copy.deepcopy(records[idx % original_len])
        if "id" in sample:
            sample["id"] = f"{sample['id']}_{idx + 1}"
        if "logId" in sample:
            sample["logId"] = sample["logId"] + idx + 1
        result.append(sample)
    return result


def sort_records_by_cursor(
    records: list[dict[str, Any]], cursor_path: str | None
) -> list[dict[str, Any]]:
    """Order captured records chronologically using the dlt incremental cursor."""
    if not cursor_path:
        return records

    keyed_records = [
        (get_nested_value(record, cursor_path), index, record)
        for index, record in enumerate(records)
    ]
    if any(value is None for value, _, _ in keyed_records):
        return records
    try:
        ordered = sorted(keyed_records, key=lambda item: (item[0], item[1]))
    except TypeError:
        return records
    return [record for _, _, record in ordered]


def looks_like_datetime_value(value: Any) -> bool:
    """Heuristic for ISO-date, ISO-datetime, and epoch-like values used by API responses."""
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    if not stripped:
        return False
    if stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
        return True
    return any(token in stripped.lower() for token in ("-", ":", "t", "z", "202", "201"))


def infer_timestamp_keys(value: Any, prefix: str = "") -> list[str]:
    """Recursively discover date/time-like keys in a payload instead of hardcoding a field list."""
    keys: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if looks_like_datetime_value(item):
                keys.append(full_key)
            keys.extend(infer_timestamp_keys(item, prefix=full_key))
    elif isinstance(value, list):
        for item in value:
            keys.extend(infer_timestamp_keys(item, prefix=prefix))
    return keys


def advance_timestamps(record: dict[str, Any], cursor_path: str | None = None) -> dict[str, Any]:
    """Advance timestamp/date values in a record using the resource cursor field when available."""
    rec = copy.deepcopy(record)
    candidate_keys = set(infer_timestamp_keys(rec))

    if cursor_path:
        cursor_leaf = cursor_path.split(".")[-1]
        candidate_keys.add(cursor_leaf)
        candidate_keys.add(cursor_leaf.replace("__", ""))

    def _should_advance(key: str) -> bool:
        key_lower = key.lower()
        if key in candidate_keys:
            return True
        tail = key_lower.split(".")[-1]
        return (
            tail.startswith("date")
            or tail.startswith("time")
            or tail.startswith("updated")
            or tail.startswith("created")
            or tail.startswith("modified")
            or tail.startswith("edited")
            or tail.startswith("start")
            or tail.startswith("end")
        )

    for key, val in rec.items():
        if isinstance(val, dict):
            rec[key] = advance_timestamps(val, cursor_path=cursor_path)
        elif isinstance(val, str) and _should_advance(key) and looks_like_datetime_value(val):
            if "202" in val or "201" in val:
                rec[key] = re.sub(
                    r"20\d{2}",
                    lambda match: str(int(match.group()) + 1),
                    val,
                    count=1,
                )
    return rec


def process_and_split_payload(
    payload: dict[str, Any],
    endpoint_name: str,
    is_paginated_or_incremental: bool = True,
    incremental_cursor_path: str | None = None,
    paginator_cursor_path: str | None = None,
    paginator_has_more_path: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Process payload. If resource is paginated/incremental, split into 3-file pattern.

    Otherwise export as single scrubbed JSON file.

    Records are padded, sorted, split, and scrubbed exactly once each; the
    shared response metadata is scrubbed once and reused across the outputs.
    """
    if not is_paginated_or_incremental:
        return {f"{endpoint_name}.json": scrub_api_response(payload)}

    data_field = detect_data_field(payload)
    if not data_field:
        return {f"{endpoint_name}.json": scrub_api_response(payload)}

    records = pad_records(payload.get(data_field, []), target_count=6)
    records = sort_records_by_cursor(records, incremental_cursor_path)
    if len(records) < 6:
        raise ValueError(
            f"Captured paginated resource {endpoint_name!r} contains no records to structure"
        )

    metadata = scrub_api_response({k: v for k, v in payload.items() if k != data_field})

    def _build_page(slice_records: list[dict[str, Any]]) -> dict[str, Any]:
        page = copy.deepcopy(metadata)
        page[data_field] = scrub_api_response(slice_records)
        return page

    # 1. run1_page1 (3 records + pagination indicators)
    page1_payload = _build_page(records[0:3])
    update_pagination_fields(
        page1_payload,
        endpoint_name=endpoint_name,
        page_1=True,
        paginator_cursor_path=paginator_cursor_path,
        paginator_has_more_path=paginator_has_more_path,
    )

    # 2. run1_page2 (2 records + cleared pagination indicators)
    page2_payload = _build_page(records[3:5])
    update_pagination_fields(
        page2_payload,
        endpoint_name=endpoint_name,
        page_1=False,
        paginator_cursor_path=paginator_cursor_path,
        paginator_has_more_path=paginator_has_more_path,
    )

    # 3. run2 (1 record with updated incremental timestamp/cursor)
    run2_record = advance_timestamps(records[5], cursor_path=incremental_cursor_path)
    run2_payload = _build_page([run2_record])
    update_pagination_fields(
        run2_payload,
        endpoint_name=endpoint_name,
        page_1=False,
        paginator_cursor_path=paginator_cursor_path,
        paginator_has_more_path=paginator_has_more_path,
    )

    return {
        f"{endpoint_name}-run1_page1.json": page1_payload,
        f"{endpoint_name}-run1_page2.json": page2_payload,
        f"{endpoint_name}-run2.json": run2_payload,
    }


def parse_endpoint_info(
    url: str,
    sources_map: dict[str, dict[str, Any]],
    *,
    _matchers: list[tuple["re.Pattern[str]", int, dict[str, Any]]] | None = None,
) -> tuple[str | None, bool, str | None, str | None, str | None]:
    """Resolve the response to a dlt resource name and pagination metadata without hardcoded source branches."""
    matched = match_resource_for_url(url, sources_map, _matchers=_matchers)
    if not matched:
        return None, True, None, None, None

    resource_name = matched.get("resource_name")
    if not resource_name:
        return None, True, None, None, None

    endpoint_name = endpoint_name_from_resource_name(resource_name)
    return (
        endpoint_name,
        matched.get("is_paginated_or_incremental", True),
        matched.get("incremental_cursor_path"),
        matched.get("paginator_cursor_path"),
        matched.get("paginator_has_more_path"),
    )


def _build_source(source_key: str, config: dict[str, Any], session: requests.Session) -> Any:
    """Instantiate a dlt source from its registry entry (dynamic dispatch)."""
    if source_key == "hubspot":
        from pipelines.sources.hubspot import hubspot_source

        return hubspot_source(session=session, **config["source_kwargs"])
    if source_key == "notion":
        from pipelines.sources.notion import notion_source

        return notion_source(session=session, **config["source_kwargs"])
    if source_key == "google_health":
        from pipelines.sources.google_health import (
            get_google_health_token,
            google_health_source,
        )

        try:
            access_token = get_google_health_token()
        except Exception as e:
            logger.warning(f"Skipping Google Health: Failed to refresh token ({e}).")
            return None
        return google_health_source(access_token=access_token, session=session)
    raise KeyError(f"Unknown mock export source: {source_key!r}")


def run_source_export(
    session: requests.Session,
    sources_map: dict[str, dict[str, Any]],
    source_key: str,
    dry_run: bool,
) -> None:
    """Run one registry-configured source extraction and capture API responses.

    Adding a new source only requires a ``SOURCE_REGISTRY`` entry (plus a
    ``_build_source`` branch while source factories differ in auth shape).
    """
    config = SOURCE_REGISTRY[source_key]
    display = config.get("display_name", source_key)
    if not get_secret(config["secret_key"]):
        logger.warning(f"Skipping {display}: {config['secret_key']} secret is not configured.")
        return

    logger.info("Executing %s source to capture API responses...", display)
    pipeline = dlt.pipeline(pipeline_name=config["pipeline_name"], destination="duckdb")
    try:
        source = _build_source(source_key, config, session)
    except KeyError:
        raise
    except Exception as e:
        logger.warning(f"Skipping {display}: Failed to initialize source ({e}).")
        return
    if source is None:
        return

    collect_source_resource_configs(source, sources_map)
    page_size_property = config.get("page_size_property")
    if page_size_property:
        override_incremental_resource_property(source, page_size_property)
    override_source_paginators(source)
    try:
        pipeline.extract(source)
    except Exception as e:
        logger.warning(f"{display} extraction completed with exception: {e}")

    try:
        list(source.resources)
    except Exception:
        logger.debug("Unable to enumerate source resources", exc_info=True)


def run_hubspot_export(
    session: requests.Session, sources_map: dict[str, dict[str, Any]], dry_run: bool
) -> None:
    """Run HubSpot source extraction and process response bodies."""
    run_source_export(session, sources_map, "hubspot", dry_run)


def run_notion_export(
    session: requests.Session, sources_map: dict[str, dict[str, Any]], dry_run: bool
) -> None:
    """Run Notion source extraction and process response bodies."""
    run_source_export(session, sources_map, "notion", dry_run)


def run_google_health_export(
    session: requests.Session, sources_map: dict[str, dict[str, Any]], dry_run: bool
) -> None:
    """Run Google Health source extraction and process response bodies."""
    run_source_export(session, sources_map, "google_health", dry_run)


def save_captured_responses(
    captured: list[tuple[str, Any, dict[str, Any]]],
    sources_map: dict[str, dict[str, Any]],
    dry_run: bool,
) -> None:
    """Save captured response bodies to tests/fixtures/[system] in standardized format."""
    processed_count = 0
    matchers = build_matchers(sources_map)

    for url, payload, headers in captured:
        if not isinstance(payload, dict):
            continue

        (
            endpoint_name,
            is_paginated_or_incremental,
            incremental_cursor_path,
            paginator_cursor_path,
            paginator_has_more_path,
        ) = parse_endpoint_info(url, sources_map, _matchers=matchers)
        if not endpoint_name:
            continue

        system_name = mock_system_name(endpoint_name)
        target_dir = MOCK_DATA_DIR / system_name
        target_dir.mkdir(parents=True, exist_ok=True)

        files_map = process_and_split_payload(
            payload,
            endpoint_name,
            is_paginated_or_incremental=is_paginated_or_incremental,
            incremental_cursor_path=incremental_cursor_path,
            paginator_cursor_path=paginator_cursor_path,
            paginator_has_more_path=paginator_has_more_path,
        )
        for filename, file_payload in files_map.items():
            file_path = target_dir / filename
            if dry_run:
                logger.info(f"[DRY-RUN] Would write: {file_path}")
            else:
                logger.info(f"Writing mock response file: {file_path}")
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(file_payload, f, indent=2, ensure_ascii=False)
            processed_count += 1

    logger.info(f"Successfully processed {processed_count} mock files.")


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser (extracted for testability)."""
    parser = argparse.ArgumentParser(
        description="Extract live API responses via dlt sources, scrub PII, and generate test mocks."
    )
    parser.add_argument(
        "--source",
        choices=[*SOURCE_REGISTRY, "all"],
        default="all",
        help="Source API to capture responses from (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process and scrub captured responses without writing to disk",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    session = ResponseCaptureSession()
    sources_map: dict[str, dict[str, Any]] = {}

    for source_key in SOURCE_REGISTRY:
        if args.source in (source_key, "all"):
            run_source_export(session, sources_map, source_key, args.dry_run)

    save_captured_responses(session.captured_responses, sources_map, args.dry_run)


if __name__ == "__main__":
    main()
