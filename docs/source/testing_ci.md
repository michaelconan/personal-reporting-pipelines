# Testing and CI

Python tests and dbt workflows are run through the project tasks in
[`mise.toml`](mise_tasks.md). CI uses the same tasks as local development.

## Python tests

```bash
mise run test          # offline tests using fixtures and DuckDB
mise run test-live     # opt-in live APIs, loading to temporary DuckDB
mise run test-e2e      # live APIs and Databricks
mise run test-all      # offline tests followed by e2e
```

Live API integration tests call real sources and can use API quota. The e2e
task starts the configured Databricks SQL warehouse and waits until it is
healthy before invoking pytest.
The e2e fixture drops its catalog-qualified, dev-mode test schema after the
class completes and resets local dlt state in a `finally` block.

## dbt tests and development

```bash
mise run dbt-build-mock
mise run dbt-build-dev
mise run dbt-build-prod
mise run dbt -- run --target mock --select stg_google_health__sleep
mise run dbt -- test --target mock --select stg_google_health__sleep
```

The `mock` target uses DuckDB and CSV fixtures. The dev/prod tasks use the
Databricks profiles and perform a warehouse preflight. For development docs,
run `mise run docs`.

## Linting and governance

```bash
mise run lint
mise run dbt-deps
mise run dbt-lint
mise run dbt-parse-mock
mise run dbt-compile-mock
mise run dbt-bouncer
```

`dbt lint` is dbt 2.x's built-in SQLFluff-compatible linter. The Python
`sqlfluff-templater-dbt` packages are not installed because they require
`dbt-core` and cannot coexist with this project's dbt 2.x distribution.

## 1Password and Databricks in CI

Credentialed mise tasks use `op run --environment "$OP_ENVIRONMENT_ID" -- ...`.
Configure `OP_SERVICE_ACCOUNT_TOKEN` as a GitHub secret and `OP_ENVIRONMENT_ID`
as a repository variable. The 1Password Environment must export the dlt source
and destination variables. CI installs the 1Password CLI `latest-beta` until
Environment support is generally available.

The Databricks CLI task starts a stopped SQL warehouse and polls for `RUNNING`
and `HEALTHY` before e2e, pipeline, or Databricks dbt tasks proceed.

## GitHub Actions

Workflows are in `.github/workflows/`:

- `refresh-notion.yml`, `refresh-hubspot.yml`, `refresh-google-health.yml` —
  scheduled ingestion via `mise run pipeline`.
- `run-transforms.yml` — scheduled production dbt freshness/build via mise.
- `test-pipelines.yml`, `test-transforms.yml`, `lint.yml` — CI tests and lint.
- `docs.yml` — builds and deploys the Sphinx/dbt docs site.

Workflow source/API credentials are supplied by the configured 1Password
Environment; mock/unit tests do not require cloud credentials.
