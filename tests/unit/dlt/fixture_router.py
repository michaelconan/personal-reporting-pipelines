"""Reusable cursor-aware HTTP fixture routing for dlt source tests."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlparse

from tests.unit.dlt.conftest import MOCK_ROOT


def get_path(value: Any, path: str | None) -> Any:
    """Read a dotted path from a JSON-compatible value."""
    if not path:
        return value
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


@dataclass
class RecordedRequest:
    method: str
    url: str
    json_body: dict[str, Any] | None
    query: dict[str, list[str]]


@dataclass
class FixtureRoute:
    name: str
    url_pattern: Any
    files: list[str]
    data_field: str
    cursor_path: str | None = None
    request_cursor_path: str | None = None
    request_cursor_location: str = "json"
    requests: list[RecordedRequest] = field(default_factory=list)
    _last_cursor: Any = None

    def response(self, request: Any) -> tuple[int, dict[str, str], str]:
        body = json.loads(request.body) if request.body else None
        query = parse_qs(urlparse(request.url).query)
        recorded = RecordedRequest(request.method, request.url, body, query)
        self.requests.append(recorded)

        request_cursor = (
            get_path(body, self.request_cursor_path)
            if self.request_cursor_location == "json"
            else query.get(self.request_cursor_path or "", [None])[0]
        )
        expected_index = len(self.requests) - 1
        if expected_index >= len(self.files):
            raise AssertionError(f"Unexpected extra page request for {self.name}")
        expected_cursor = self._last_cursor if expected_index else None
        if request_cursor != expected_cursor:
            raise AssertionError(
                f"Unexpected request cursor for {self.name}: "
                f"expected {expected_cursor!r}, got {request_cursor!r}"
            )

        payload = json.loads((MOCK_ROOT / self.files[expected_index]).read_text())
        self._last_cursor = get_path(payload, self.cursor_path)
        return 200, {"Content-Type": "application/json"}, json.dumps(copy.deepcopy(payload))


class FixtureRouter:
    """Register explicit routes and retain every request for assertions."""

    def __init__(self, mock_responses: Any) -> None:
        self.mock_responses = mock_responses
        self.routes: dict[str, FixtureRoute] = {}

    def add(self, route: FixtureRoute) -> FixtureRoute:
        self.routes[route.name] = route
        self.mock_responses.add_callback(
            self.mock_responses.POST,
            route.url_pattern,
            callback=route.response,
            content_type="application/json",
        )
        return route

    def requests_for(self, name: str) -> list[RecordedRequest]:
        return self.routes[name].requests


def load_fixture_manifest() -> dict[str, Any]:
    import yaml

    return yaml.safe_load((MOCK_ROOT / "manifest.yml").read_text())
