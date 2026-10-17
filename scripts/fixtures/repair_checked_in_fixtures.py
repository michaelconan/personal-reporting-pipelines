"""Repair checked-in fixtures after fixing the capture generator.

This is intentionally deterministic and offline.  It is useful when the live
API is unavailable but the generator's fixture invariants need to be applied
to an existing capture set.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures"


def read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n")


def repair_google_health(resource: str) -> None:
    prefix = FIXTURES / "google_health" / f"google_health__{resource}"
    pages = [
        read(prefix.with_name(prefix.name + suffix))
        for suffix in (
            "-run1_page1.json",
            "-run1_page2.json",
            "-run2.json",
        )
    ]
    seen: set[str] = set()
    for page_index, page in enumerate(pages):
        for record_index, record in enumerate(page["dataPoints"], start=1):
            name = record.get("name")
            if not isinstance(name, str) or not name or name in seen:
                name = f"mock-google-health-{resource}-{page_index + 1}-{record_index}"
            elif resource == "steps":
                name = f"mock-google-health-steps-{page_index + 1}-{record_index}"
            record["name"] = name
            seen.add(name)
    for page, suffix in zip(pages, ("-run1_page1.json", "-run1_page2.json", "-run2.json")):
        write(prefix.with_name(prefix.name + suffix), page)


def repair_hubspot(resource: str) -> None:
    path = FIXTURES / "hubspot" / f"hubspot__{resource}-run1_page1.json"
    payload = read(path)
    payload.setdefault("paging", {}).setdefault("next", {})["after"] = str(
        payload["results"][-1]["id"]
    )
    write(path, payload)


def main() -> None:
    for resource in ("sleep", "steps", "exercise"):
        repair_google_health(resource)
    for resource in (
        "contacts",
        "companies",
        "meetings",
        "calls",
        "communications",
        "tasks",
        "notes",
        "deals",
        "tickets",
    ):
        repair_hubspot(resource)


if __name__ == "__main__":
    main()
