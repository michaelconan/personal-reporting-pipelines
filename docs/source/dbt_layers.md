# dbt Layers and Conventions

This page explains the dbt layering strategy used in this project and key conventions to follow.

## Layering

- Staging (`dbt/models/staging/`): lightweight, source-facing models that normalize raw table shapes and expose consistent column names. Files are `stg_{source}__{entity}.sql`.
- Core (`dbt/models/core/`): generic, source-agnostic entities that could be produced by comparable source systems (for example: `core_habit_events.sql`, `core_sleep_sessions.sql`, `core_daily_steps.sql`, `core_exercise_sessions.sql`).
- Intermediate (`dbt/models/intermediate/`): merge, reshape, and transform across core entities into the reporting grain. Example: `habits/int_habits.sql`.
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
- `seconds_between(start_ts, end_ts)` — cross-db timestamp difference in seconds
- `date_from_parts(year, month, day)` — cross-db date construction
- `date_from_offset_seconds(ts, offset)` — local date from a UTC timestamp plus a seconds offset string
- `unnest_json_array(array_col, alias)` — JSON array unnest helper

When adding macros, register them under `dbt/macros/` and document usage in this page.

## Variables

Therapeutic/completion thresholds are **not** encoded as dbt vars: they come from the `stg_notion__habit_reference` model at the metrics layer.

Useful dbt vars defined for transforms:

- `dbt_date:time_zone` — 'UTC' (used by dbt date utilities)

## Running locally

- To build dev target (DuckDB + seeds):

```bash
make dbt-build target=dev
# or run a single model
make dbt-run target=dev select="stg_google_health__sleep"
```

- Generate docs:

```bash
pipenv run dbt docs generate
pipenv run dbt docs serve
```

## Tests

- Unit/integration SQL tests live in `dbt/tests/` and are executed during CI and in `make dbt-build`.
- Use `sqlfluff lint --dialect duckdb` for SQL style checks when working against the dev target.
