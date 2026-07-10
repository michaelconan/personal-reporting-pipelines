# dbt Layers and Conventions

This page explains the dbt layering strategy used in this project and key conventions to follow.

## Layering

- Staging (`dbt/models/staging/`): lightweight, source-facing models that normalize raw table shapes and expose consistent column names. Files are `stg_{source}__{entity}.sql`.
- Intermediate (`dbt/models/intermediate/`): transformations shared across sources (for example: unpivoting Notion checkboxes into long format). Example: `int_habits_unpivoted.sql`.
- Marts (`dbt/models/marts/`): analytics-ready tables and aggregates consumed by BI/metrics. Examples: `habits/habits_v1.sql`, `habits/habits_metrics_v1.sql`.

## make_source macro

The macro `make_source(source_name, relation_name)` adapts to environment:

- In dev (DuckDB) it returns `ref('{source_name}__{relation_name}')` so dbt reads mock seed files.
- In prod/test (BigQuery) it returns `source(source_name, relation_name)` to reference the raw BigQuery schema.

Consequence: local seed filenames must match the source identifier names in `dbt/seeds/mock_sources/` (e.g., `notion__data_source_daily_habits.csv`).

## Seeds

- `dbt/seeds/discipline_reference.csv` — canonical master list of personal disciplines, targets and thresholds.
- `dbt/seeds/mock_sources/` — mock source files used for local development and DuckDB target. These seeds replicate the raw table naming convention.

## Macros and utilities

Key project macros are under `dbt/macros/` and include:

- `json_extract_value(column, path)` — cross-db JSON extraction (works for BigQuery and DuckDB)
- `trunc_date(period, date_expr)` — cross-db date truncation
- `cast_safe(expr, type)` — safe casting helper
- `unnest_json_array(array_col, alias)` — JSON array unnest helper

When adding macros, register them under `dbt/macros/` and document usage in this page.

## Variables

Useful dbt vars defined for transforms:

- `sleep_goal` — default: 25200000 (7 hours in ms)
- `steps_goal` — default: 7500
- `meet_goal` — default: 1 (discipline_reference threshold may be 2)

## Running locally

- To build dev target (DuckDB + seeds):

```bash
make dbt-build target=dev
# or run a single model
make dbt-run target=dev select="stg_fitbit__sleep"
```

- Generate docs:

```bash
pipenv run dbt docs generate
pipenv run dbt docs serve
```

## Tests

- Unit/integration SQL tests live in `dbt/tests/` and are executed during CI and in `make dbt-build`.
- Use `sqlfluff lint --dialect duckdb` for SQL style checks when working against the dev target.
