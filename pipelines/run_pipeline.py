"""CLI runner for dlt pipelines.

Usage examples:

python -m pipelines.run_pipeline notion \
    --select notion__data_source_rows --full
python -m pipelines.run_pipeline hubspot \
    --select hubspot__contacts,hubspot__companies \
    --incremental --initial-date 2023-01-01
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List

from pipelines.runner import PIPELINE_CONFIG, refresh_pipeline

logger = logging.getLogger(__name__)


def parse_select(values: List[str] | None) -> list[str] | None:
    if not values:
        return None
    # support repeated --select and comma-separated values
    out: list[str] = []
    for v in values:
        parts = [p.strip() for p in v.split(",") if p.strip()]
        out.extend(parts)
    return out if out else None


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_pipeline")
    parser.add_argument(
        "pipeline",
        choices=list(PIPELINE_CONFIG.keys()),
        help="Pipeline to run",
    )
    parser.add_argument(
        "--select",
        action="append",
        help="Resource name to select (can be repeated or comma-separated)",
    )
    parser.add_argument(
        "--incremental",
        dest="incremental",
        action="store_true",
        help="Force incremental refresh",
    )
    parser.add_argument(
        "--full",
        dest="incremental",
        action="store_false",
        help="Force full refresh",
    )
    parser.set_defaults(incremental=None)
    parser.add_argument(
        "--initial-date",
        help="Initial date for incremental loads",
    )
    parser.add_argument("--end-date", help="End date for incremental loads")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG if args.debug else logging.INFO,
    )

    is_incremental = args.incremental

    select = parse_select(args.select)

    try:
        result = refresh_pipeline(
            args.pipeline,
            is_incremental=is_incremental,
            initial_date=args.initial_date,
            end_date=args.end_date,
            select=select,
        )
        logger.info("Pipeline finished: %s", result)
    except KeyError:
        logger.error("Unknown pipeline: %s", args.pipeline)
        return 2
    except Exception as e:
        logger.exception("Pipeline run failed: %s", e)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
