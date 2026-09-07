# scripts/fixtures/scrub_data.py
"""
Data scrubbing utilities using Faker to sanitize PII and sensitive text
in mock dbt seeds and mock API response bodies.

Field matching is dynamic: besides an explicit set of known dlt column names,
keys are classified by suffix heuristics (names, emails, free-text bodies), so
new sources and endpoints are scrubbed without code changes.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from faker import Faker

fake = Faker()

# Exact dlt column names known to carry free text or PII.
TEXT_KEYS = {
    "properties__notes__rich_text",
    "properties__description__rich_text",
    "properties__hs_note_body",
    "properties__subject",
    "properties__hs_task_body",
    "properties__hs_call_title",
    "properties__hs_call_body",
    "properties__hs_meeting_title",
    "properties__hs_meeting_body",
    "properties__hs_internal_meeting_notes",
    "properties__hs_communication_body",
    "properties__name",
    "properties__dealname",
    "properties__content",
    "hs_note_body",
    "subject",
    "hs_task_body",
    "hs_call_title",
    "hs_call_body",
    "hs_meeting_title",
    "hs_meeting_body",
    "hs_internal_meeting_notes",
    "hs_communication_body",
    "dealname",
    "content",
    "description",
}

# Suffix heuristics for keys not in TEXT_KEYS. The leaf segment (after the
# last "__" or ".") is matched, so nested dlt columns classify dynamically.
_FREE_TEXT_SUFFIXES = (
    "title",
    "subject",
    "body",
    "content",
    "description",
    "notes",
    "dealname",
    "comment",
    "message",
)
_NAME_SUFFIXES = ("firstname", "first_name", "lastname", "last_name", "name")
_EMAIL_SUFFIXES = ("email", "user_email")
# Structural/metadata keys that must never be treated as free text.
_EXCLUDED_KEYS = {
    "id",
    "log_id",
    "logid",
    "filename",
    "pathname",
    "hostname",
    "username",
    "domain",
    "timezone",
    "time_zone",
}

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def seed_faker(seed: int) -> None:
    """Seed the shared Faker instance for deterministic scrubbed output in tests."""
    fake.seed_instance(seed)


def resolve_project_root(start: Path | None = None) -> Path:
    """Walk upward from ``start`` (or this file) to the repo root.

    The root is identified by ``pyproject.toml`` instead of a fixed parent
    depth, so the helper keeps working if the module moves within the repo.
    """
    current = Path(start) if start is not None else Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    # Fallback for direct script execution outside a checkout layout.
    return Path(__file__).resolve().parents[2]


def _leaf(key: str) -> str:
    """Return the final segment of a dlt ``__``-joined or dotted key path."""
    return re.split(r"(?:__|\\.)", key)[-1].lower()


def _is_free_text_key(key: str) -> bool:
    """Check whether a key carries scrub-worthy free text (exact or suffix match)."""
    if key in TEXT_KEYS:
        return True
    leaf = _leaf(key)
    if leaf in _EXCLUDED_KEYS:
        return False
    return leaf in _FREE_TEXT_SUFFIXES or leaf.endswith(
        ("_body", "_title", "_notes", "_description", "_content", "_subject")
    )


def _is_name_key(key: str) -> bool:
    """Check whether a key carries a person or entity name."""
    return _leaf(key) in _NAME_SUFFIXES


def _is_email_key(key: str) -> bool:
    """Check whether a key carries an email address."""
    leaf = _leaf(key)
    return leaf in _EMAIL_SUFFIXES


def apply_fakes_to_rows(data_rows: list[dict]) -> list[dict]:
    """Replace designated columns in BigQuery/seed rows with realistic fake data.

    Rows are scrubbed in place and also returned for convenient chaining.

    Args:
        data_rows: List of dictionary rows to scrub.

    Returns:
        The same list of rows, scrubbed.
    """
    for row in data_rows:
        for col in list(row.keys()):
            value = row[col]
            if value is None:
                continue
            if col == "properties__firstname" or _leaf(col) == "firstname":
                row[col] = fake.first_name()
            elif col == "properties__lastname" or _leaf(col) == "lastname":
                row[col] = fake.last_name()
            elif col == "properties__email" or _is_email_key(col):
                row[col] = fake.email()
            elif _is_free_text_key(col):
                row[col] = fake.sentence(nb_words=12)
    return data_rows


def _scrub_value(key: str, value: str) -> str:
    """Scrub a single string value based on its key and content shape."""
    if _is_email_key(key) or _EMAIL_RE.match(value):
        return fake.email()
    if _leaf(key) in ("firstname", "first_name"):
        return fake.first_name()
    if _leaf(key) in ("lastname", "last_name"):
        return fake.last_name()
    if _is_name_key(key):
        return fake.first_name()
    if _is_free_text_key(key):
        return fake.sentence(nb_words=8)
    return value


def scrub_api_response(data: dict | list | Any) -> Any:
    """Recursively scrub PII and free-text fields in JSON API response objects.

    Args:
        data: The JSON dictionary or list to scrub.

    Returns:
        The scrubbed JSON structure.
    """
    if isinstance(data, list):
        return [scrub_api_response(item) for item in data]
    if isinstance(data, dict):
        scrubbed: dict[str, Any] = {}
        for key, val in data.items():
            if isinstance(val, (dict, list)):
                scrubbed[key] = scrub_api_response(val)
            elif isinstance(val, str) and val:
                scrubbed[key] = _scrub_value(key, val)
            else:
                scrubbed[key] = val
        return scrubbed
    return data
