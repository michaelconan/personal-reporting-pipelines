# dbt Layers and Conventions

This page explains the dbt layering strategy used in this project and key conventions to follow.

## Layering

- Staging (`dbt/models/staging/`): lightweight, source-facing models that normalize raw table shapes and expose consistent column names. Files are `stg_{source}__{entity}.sql`.
- Core (`dbt/models/core/`): generic, source-agnostic entities that could be produced by comparable source systems (for example: `core_habit_events.sql`, `core_sleep_sessions.sql`, `core_daily_steps.sql`, `core_exercise_sessions.sql`).
- Intermediate (`dbt/models/intermediate/`): merge, reshape, and transform across core entities into the reporting grain. Example: `habits/int_habits.sql`.
- Marts (`dbt/models/marts/`): analytics-ready conformed dimensions and facts consumed by BI/metrics, stored flat in the directory. Examples: `dim_habit_v1.sql`, `fct_habit_occurrence_v1.sql`.

## Model structure

Every SQL model follows the same top-to-bottom shape so it is easy to debug:

1. **Import CTEs at the top** — one CTE per `ref()`/`source()`, and the only place `ref()` is called.
2. **Transform CTEs next** — all joins, casts, derivations, and filtering as named CTEs.
3. **`final` CTE + `select * from final` last** — the last CTE holds the output shape and the model ends with exactly `select * from final`.

```sql
with
stg_hubspot__contacts as (
    select * from {{ ref('stg_hubspot__contacts') }}
),
final as (
    select contact_id, email from stg_hubspot__contacts
)
select * from final
```

The convention applies to staging, core, intermediate, and marts. Union models align their branches in CTEs and union inside `final`.

## Source resolution (no make_source macro)

Models call `{{ source(source_name, relation_name) }}` directly; there is no `make_source` macro:

- In mock (DuckDB) each source reads its test fixture via `config.external_location: dbt/test_fixtures/{source}/{identifier}.csv`.
- In dev/test/prod (BigQuery) sources resolve to the raw schema.

Consequence: fixture filenames under `dbt/test_fixtures/{source}/` must match the source identifiers in `_*sources.yml`.

## Seeds

- `dbt/seeds/group_connect_cadence.csv` — target connection cadence per group tier (`cadence_value` + `cadence_period`), for all environments.
- `dbt/test_fixtures/{source}/*.csv` — mock source files used for local development and the DuckDB `mock` target; these follow the raw table naming convention.

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
uv run dbt docs generate --project-dir dbt --profiles-dir dbt
uv run dbt docs serve --project-dir dbt --profiles-dir dbt
```

## Tests

- Unit/integration SQL tests live in `dbt/tests/` and are executed during CI and in `make dbt-build`.
- Use `uv run dbt lint --project-dir dbt --profiles-dir dbt --target mock` for SQL style checks when working against the dev target.
