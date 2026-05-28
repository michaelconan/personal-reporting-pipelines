# dlt Pipelines

This page describes the dlt-based extraction pipelines, how they map to raw tables, and operational notes.

## Overview

The repository defines three dlt extraction pipelines (pipelines/):

- pipelines/notion.py — Notion "habits" extraction
- pipelines/hubspot.py — HubSpot CRM extraction (per-object)
- pipelines/fitbit.py — Fitbit activity and sleep extraction

Shared helpers and configuration live under pipelines/common/ and `pipelines/__init__.py`.

## Raw table naming

dlt writes raw tables with the convention: `{source}__{entity}` (e.g., `hubspot__contacts`, `fitbit__sleep`, `notion__data_source_daily_habits`).

- Notion pipelines use `data_source` identifiers. Key columns:
  - `parent__data_source_id` (Notion parent id)
  - `properties__{field}__checkbox|number|date|formula` (property payloads)

- HubSpot uses per-object tables (meetings, calls, tasks, notes, etc.) and association tables with `_to_contacts` style names.

- Fitbit uses `fitbit__sleep` (sleep events) and `fitbit__activities` (steps, durations).

## Running pipelines

Standard pattern (pipenv):

```bash
# run a pipeline module (uses environment for creds/profile)
pipenv run python -m pipelines.hubspot
```

Programmatic usage (for local dev/test):

```python
from pipelines.hubspot import refresh_hubspot
# default: detects incremental behavior from env
refresh_hubspot(is_incremental=None)
# force full reload
refresh_hubspot(is_incremental=False)
```

## Refresh modes and overrides

Three ways to force a full refresh:

- Global: set FORCE_FULL_REFRESH=true
- Per-pipeline: set PIPELINE_NAME and {PIPELINE_NAME}_FULL_REFRESH=true
- Function param: call refresh_* with is_incremental=False

Examples:

```bash
export FORCE_FULL_REFRESH=true
pipenv run python -m pipelines.fitbit

# Or pipeline specific
export PIPELINE_NAME=HUBSPOT
export HUBSPOT_FULL_REFRESH=true
pipenv run python -m pipelines.hubspot
```

## Config and secrets

- HubSpot object/property config: pipelines/hs_config.yml
- Credentials and API keys: use GCP Secret Manager or local env vars depending on `SECRET_STORE` configuration.

## Operational notes

- Logs and incremental state are managed by dlt; keep an eye on the dlt state folder if debugging.
- For local development, use the `dbt/seeds/mock_sources/` CSV files to mimic raw data when using the dev (DuckDB) dbt target.
- If schema changes are expected, prefer a controlled full refresh and run dbt tests afterwards.
