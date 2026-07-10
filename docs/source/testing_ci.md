# Testing and CI

This page explains local tests, dbt testing, linting, and CI workflows.

## Local Python tests

- Run unit tests:

```bash
make test-local
```

This runs the repository Python unit tests (pipenv environment expected).

## dbt tests

- Execute dbt models and tests locally (dev target):

```bash
make dbt-build target=dev
# or run specific models and tests
pipenv run dbt run --select stg_fitbit__sleep
pipenv run dbt test --select stg_fitbit__sleep
```

- Generate docs:

```bash
pipenv run dbt docs generate
pipenv run dbt docs serve
```

## SQL linting

- SQLFluff config is present. Lint against DuckDB dialect for local development:

```bash
sqlfluff lint --dialect duckdb
```

## GitHub Actions

Workflows are in `.github/workflows/` and include:

- `refresh-notion.yml`, `refresh-hubspot.yml`, `refresh-fitbit.yml` — ingestion schedules
- `run-transforms.yml` / `test-transforms.yml` — dbt transform runs and dbt tests
- `docs.yml` — builds and deploys Sphinx docs
- `weekly-doc-updater.lock.yml` (generated from the gh-aw workflow source) — agentic workflow that opens PRs to keep docs in sync

CI notes:

- CI runs dbt tests and unit tests; fix failures locally before opening PRs.
- When modifying seeds/macros, ensure both dev and prod (DuckDB and BigQuery) compatibility.
