# Seeds and Macros

This page documents seed usage and important dbt macros in the project.

## Seeds

Seeds provide deterministic inputs for local development and testing.

- Location: `dbt/seeds/` and `dbt/seeds/mock_sources/`
- Naming: mock source seeds must follow `{source}__{table}.csv` to align with `make_source` behavior.

Key seed files:

- `discipline_reference.csv` — canonical discipline keys, thresholds and targets used by marts and metrics
- `mock_sources/*` — per-source mock raw data used for local development with the DuckDB target

Adding a new mock source record:
1. Create `{source}__{table}.csv` in `dbt/seeds/mock_sources/` with headers matching the raw JSON payload shape
2. Run `make dbt-run target=dev select=stg_{source}__{table}` to validate

## Macros

Macros centralize cross-database logic. Important macros:

- `json_extract_value(column, path)` — returns scalar values from JSON across BigQuery and DuckDB
- `trunc_date(period, date_expr)` — truncates timestamps by period (day, week, month)
- `cast_safe(expr, type)` — returns safely casted expression or null on failure
- `unnest_json_array(array_col, alias)` — helper to unnest JSON arrays in a cross-db manner

Location: `dbt/macros/` (look for `_macros__properties.yml` and `.sql` files)

## Best practices

- Seed files should be small and focused; use seed properties files (e.g., `_seeds__properties.yml`) to control quoting and header behavior.
- Keep macros well-documented and add examples near each macro definition.
- When modifying a macro, run both dev and prod-style queries (DuckDB and BigQuery) as their JSON functions differ.
