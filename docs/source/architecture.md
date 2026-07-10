# Architecture and Data Flow

High-level architecture:

Notion / HubSpot / Fitbit APIs
    ↓ (dlt pipelines → BigQuery raw schema)
dbt Staging (views) → dbt Intermediate → dbt Marts (tables)
    ↓ MetricFlow semantic layer (optional)

## Components

- dlt: extraction and raw-loading into BigQuery. Keeps incremental state and supports full refreshes.
- dbt: transformations layered into staging, intermediate, and marts. Seeds and macros enable local dev.
- BigQuery: production data warehouse. DuckDB used for local dev target.
- GitHub Actions: scheduling and orchestration for daily/weekly runs.

## Operational guidance

- Ingest first, then run dbt transforms (workflows configured to enforce this order).
- Use `make dbt-build target=dev` to validate transformations with mock seeds locally.
- Maintain discipline_reference.csv as the single source of truth for habit targets and thresholds.

## Where to look in the repo

- dlt pipelines: `pipelines/`
- dbt project: `dbt/`
- CI: `.github/workflows/`
- Agentic workflows: `.github/aw/` and `.github/workflows/*.lock.yml`
