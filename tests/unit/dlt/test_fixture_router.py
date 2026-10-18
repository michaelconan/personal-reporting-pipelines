"""Tests for cursor-driven fixture pagination."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from tests.unit.dlt.fixture_router import FixtureRoute, get_path, load_fixture_manifest


def test_fixture_route_returns_pages_in_cursor_order() -> None:
    contract = load_fixture_manifest()["resources"]["notion__data_source_rows"]
    files = [contract["files"][phase] for phase in ("run1_page1", "run1_page2")]
    route = FixtureRoute(
        name="notion rows",
        url_pattern=None,
        files=files,
        data_field=contract["data_field"],
        cursor_path=contract["cursor_path"],
        request_cursor_path=contract["request_cursor_path"],
    )

    first_response = route.response(
        SimpleNamespace(method="POST", url="https://api.notion.test/query", body="{}")
    )
    first_payload = json.loads(first_response[2])
    assert len(first_payload[contract["data_field"]]) == 3

    second_response = route.response(
        SimpleNamespace(
            method="POST",
            url="https://api.notion.test/query",
            body=json.dumps({"start_cursor": first_payload[contract["cursor_path"]]}),
        )
    )
    second_payload = json.loads(second_response[2])
    assert len(second_payload[contract["data_field"]]) == 2

    with pytest.raises(AssertionError, match="Unexpected extra page request"):
        route.response(
            SimpleNamespace(
                method="POST",
                url="https://api.notion.test/query",
                body=json.dumps(
                    {"start_cursor": get_path(second_payload, contract["cursor_path"])}
                ),
            )
        )
