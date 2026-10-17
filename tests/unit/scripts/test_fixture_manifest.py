"""Contract tests for the checked-in REST API fixtures."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.unit.dlt.fixture_router import get_path, load_fixture_manifest

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures"


def load(relative_path: str) -> dict[str, Any]:
    return json.loads((FIXTURES / relative_path).read_text())


def test_manifest_files_and_page_contracts_are_valid() -> None:
    manifest = load_fixture_manifest()

    for resource_name, contract in manifest["resources"].items():
        files = contract["files"]
        payloads = {phase: load(path) for phase, path in files.items()}
        data_field = contract["data_field"]
        identity_path = contract["identity_path"]

        run1_records = [
            record
            for phase in ("run1_page1", "run1_page2")
            for record in payloads[phase][data_field]
        ]
        identities = [get_path(record, identity_path) for record in run1_records]
        assert all(identities), f"{resource_name} contains records without identity"
        assert len(identities) == len(set(identities)), (
            f"{resource_name} repeats an identity across pagination pages"
        )

        page1_cursor = get_path(payloads["run1_page1"], contract["cursor_path"])
        assert page1_cursor, f"{resource_name} page 1 is missing its cursor"
        page2_cursor = get_path(payloads["run1_page2"], contract["cursor_path"])
        assert page2_cursor is None, f"{resource_name} page 2 must terminate pagination"

        run2_identity = get_path(payloads["run2"][data_field][0], identity_path)
        assert run2_identity not in identities, f"{resource_name} run2 is not new data"

        cursor_path = contract["incremental_cursor_path"]
        run1_cursors = [get_path(record, cursor_path) for record in run1_records]
        run2_cursor = get_path(payloads["run2"][data_field][0], cursor_path)
        assert all(run1_cursors), f"{resource_name} has missing incremental cursors"
        assert run2_cursor > max(run1_cursors), (
            f"{resource_name} run2 cursor must advance beyond run1"
        )

        if contract["cursor_path"] == "paging.next.after":
            assert str(page1_cursor) == str(run1_records[2]["id"])
