# Architecture and Data Flow

High-level architecture:

Notion / HubSpot / Google Health APIs
    ↓ (dlt pipelines → Databricks raw schema / Unity Catalog)
dbt Staging (views) → dbt Intermediate → dbt Marts (tables)
    ↓ MetricFlow semantic layer

## Components

- dlt: extraction and direct-loading into Databricks (Unity Catalog). Keeps incremental state and supports full refreshes.
- dbt: transformations layered into staging, intermediate, and marts. Seeds and macros enable local dev.
- Databricks: primary data warehouse (Unity Catalog); dlt stages data in a managed volume and loads into the raw schema. dbt transforms and marts run on Databricks. BigQuery targets retained for easy rollback path. DuckDB used for local dev target.
- GitHub Actions: scheduling and orchestration for daily/weekly runs.

## Operational guidance

- Ingest first, then run dbt transforms (workflows configured to enforce this order).
- Use `make dbt-build target=dev` to validate transformations against Databricks dev target locally.
- Use `uv run dbt build --project-dir dbt --profiles-dir dbt --target mock` for local testing with CSV fixtures.
- Maintain group_connect_cadence.csv as the single source of truth for group tier cadence targets.

## Where to look in the repo

- dlt pipelines: `pipelines/`
- dbt project: `dbt/`
- CI: `.github/workflows/`
- Agentic workflows: `.github/aw/` and `.github/workflows/*.lock.yml`
