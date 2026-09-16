# Personal Reporting

This dbt project is designed to transform and model data from various sources, including HubSpot, Notion, and Google Health. The project follows the dbt best practices for structuring a dbt project, with clear separation between staging, core, intermediate, and mart layers.

## Data Ingestion

The raw data is ingested using `dlt` (data load tool), which means that the raw tables have a specific structure that the staging models are designed to handle. The raw data is loaded into the `raw` schema in the production environment (BigQuery). For local testing, source tables are read directly from CSV test fixtures in `dbt/test_fixtures/` via DuckDB external locations (configured through the `external_location` meta on each source).

## Layers

Model layers have been implemented as recommended by [dbt's project structure guide](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

| Layer        | Description                               | Scope                                                  | Notes                                                              |
|--------------|-------------------------------------------|--------------------------------------------------------|--------------------------------------------------------------------|
| Staging      | Foundational models organised by source   | Renaming, type casting, basic computations, categorising | Standardise names to snake case, deduplicate for change data loading |
| Core         | Generic, source-agnostic data entities    | Conformed entities a comparable source system could produce | Conformance contracts remain provider-neutral                  |
| Intermediate | Apply complex transformations by focus area | Structural simplification, re-graining, merging, isolating complex operations | Contracts enforced |
| Marts        | Entity or concept layer, denormalised     | Standard entity concepts, built wide, and extended thoughtfully | Contracts enforced |

## Column Naming Standards

Column names must be `snake_case` and encode their data type, so schemas are self-describing. These standards are enforced at CI time by dbt-bouncer catalog checks (see `dbt-bouncer.yml`):

| Data type                                  | Standard                  | Examples                                  |
|--------------------------------------------|---------------------------|-------------------------------------------|
| Any                                        | `snake_case` only         | `page_id`, `habit_date`, `is_complete`    |
| `BOOLEAN`                                  | `is_`/`did_`/`has_` prefix, or `_met` suffix | `did_devotional`, `is_synchronous`, `target_met` |
| `DATE`                                     | `_date` suffix            | `habit_date`, `page_date`, `session_date` |
| `TIMESTAMP` / `TIMESTAMP WITH TIME ZONE`   | `_at` suffix              | `created_at`, `occurred_at`, `started_at` |
| `INTEGER` / `BIGINT` (integers)            | `_id`/`_count`/`_periods` suffixes, or a domain unit label (`_minutes`/`_value`/`_kcal`/`_tier`/`_level`/`_index`) | `interval_count`, `company_tier`, `asleep_minutes` |
| `DOUBLE` / `NUMERIC` (continuous metrics)  | `_minutes`/`_seconds`/`_meters`/`_pct`/`_rate`/`_value` label | `duration_minutes`, `completion_rate`     |

Conventions:

- Every `DATE` column ends with `_date`; the semantic layer's `date_day` column on `time_spine_daily` is the single documented exception.
- Every `TIMESTAMP` column ends with `_at`.
- Every `BOOLEAN` column starts with `is_`, `did_`, or `has_`, or ends with `_met`.
- Integer columns carry one of the suffixes above; booleans, dates, and timestamps are also guarded in the reverse direction (a column named with these suffixes/prefixes must have the matching data type).

## Local Testing

This project is set up with a local testing environment using DuckDB. To run the project locally, you need to:
1.  Run `uv sync` to install Python dependencies.
2.  Run `uv run dbt build --project-dir dbt --profiles-dir dbt --target mock` to exercise the models and tests against the `dbt/test_fixtures/` CSVs.
3.  Optionally run `uv run dbt docs generate --project-dir dbt --profiles-dir dbt --static --target mock` for docs/catalog and `uv run dbt-bouncer --config-file dbt/dbt-bouncer.yml` for governance checks.

The local testing setup uses a `profiles.yml` file located in `dbt/profiles`, which is configured to use a local DuckDB database file (`dbt.duckdb`).

## Data Quality

Data quality is enforced through a series of data tests defined in the `properties.yml` files. These tests include:
-   Standard generic tests (e.g., `unique`, `not_null`, `relationships`).
-   Tests from the `dbt_expectations` package.
-   Custom generic tests.

All data tests are run as part of the `dbt build` command. Governance (naming, documentation, contracts, test coverage) is enforced separately by [dbt-bouncer](https://github.com/godatadriven/dbt-bouncer), including the custom checks under `dbt/checks/`.
