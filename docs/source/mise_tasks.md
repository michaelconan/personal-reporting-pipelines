# Project commands with mise

The repository's developer and CI workflows are defined in `mise.toml`. Install
the pinned toolchain, then use the shared tasks from the repository root:

```bash
mise install
mise run setup
```

The mise `[env]` configuration sets the project/profile paths and common runtime
defaults. `.env.databricks` is deprecated and is no longer loaded. Supply
credentialed values through the selected 1Password Environment, or configure
`sbx` secrets inside Docker AI Sandbox.

## Common tasks

| Command | Purpose |
| --- | --- |
| `mise run test` | Offline Python unit tests |
| `mise run test-live` | Opt-in integration tests against live APIs and temporary DuckDB |
| `mise run test-e2e` | Live API + Databricks tests |
| `mise run test-all` | Unit tests followed by e2e tests |
| `mise run pipeline -- notion [args]` | Run one dlt source |
| `mise run pipeline-all` | Run Notion, HubSpot, and Google Health |
| `mise run dbt-build-mock` | Build/test dbt models with DuckDB fixtures |
| `mise run dbt-build` | Build/test against `DBT_TARGET` (default `dev`) |
| `mise run dbt-source-freshness` | Run source freshness against `DBT_TARGET` (default `dev`) |
| `mise run dbt -- run --target mock --select <model>` | Pass a dbt command through mise |
| `mise run dbt-lint` | Lint SQL with dbt's built-in linter |
| `mise run dbt-bouncer` | Run dbt-bouncer checks |
| `mise run docs` | Build dbt and Sphinx documentation |
| `mise run lint` | Run all prek checks |
| `mise run coverage` | Print coverage and generate HTML |
| `mise run clean` | Remove local test/build/documentation artifacts |

`DBT_TARGET` defaults to `dev`, with matching `DBT_SCHEMA=reporting_dev` and
`RAW_SCHEMA=raw_dev` defaults for local work. Set all three together when
targeting another environment; the scheduled workflows set them explicitly on
their run steps. The pipeline, `dbt-build`, and `dbt-source-freshness` tasks
validate that the target and schemas agree before running.

Credentialed tasks require `OP_SERVICE_ACCOUNT_TOKEN` and `OP_ENVIRONMENT_ID`.
They run their commands using:

```bash
op run --environment "$OP_ENVIRONMENT_ID" -- <command>
```

The selected 1Password Environment should export dlt-compatible source and
Databricks variables, including `DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME`,
`DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH`, and
`DESTINATION__DATABRICKS__CREDENTIALS__CATALOG`, plus either
`DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN` or both
`DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID` and
`DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET`. The warehouse helper maps
the dlt OAuth pair to the Databricks CLI's native `DATABRICKS_CLIENT_ID` and
`DATABRICKS_CLIENT_SECRET` variables and selects OAuth M2M authentication.
The dbt `test` and `prod` profiles read the dlt-style
`DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID` and
`DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET` variables directly; the
`dev` profile continues to use the dlt access token.

The Databricks CLI task derives the warehouse ID from the dlt HTTP path, starts
the SQL warehouse if needed, and waits for it to report `RUNNING` and `HEALTHY`.
For PAT authentication, it maps the dlt `ACCESS_TOKEN` to `DATABRICKS_TOKEN`.
`pipeline`, `pipeline-all`, `test-e2e`, `dbt-build`, and `dbt-source-freshness`
perform this preflight automatically. To start it separately, run
`mise run databricks-start`. The task command definitions are in `mise.toml`;
`scripts/databricks_warehouse.py` handles the warehouse start/wait logic and
the `docs` task builds Sphinx output and copies the dbt SPA. The dbt redirect is
the checked-in `docs/source/dbt.html` page. The e2e fixture also drops its
catalog-qualified, dev-mode schema after tests and clears local dlt state even
if remote schema deletion fails.

## GitHub Actions

Actions use `jdx/mise-action` and invoke the same tasks. Configure
`OP_SERVICE_ACCOUNT_TOKEN` as a GitHub secret and `OP_ENVIRONMENT_ID` as a
repository variable. The workflows install the 1Password CLI `latest`.
