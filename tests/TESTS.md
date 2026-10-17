# Tests

This repo has two test systems: **pytest** for `pipelines/` (dlt) and **dbt built-in tests** for `dbt/` transformations. CI runs both plus SQL/Python linting.

| Layer | Location | Destination | pytest marker | Local command | CI workflow |
| ----- | -------- | ----------- | ------------- | ------------- | ----------- |
| dlt unit (offline) | `tests/unit/dlt/` | DuckDB (`local_unit_test` / `local_data`, `dev_mode=True`) | `local` | `mise run test` (`pytest tests/unit`) | `test-pipelines.yml` (always runs) |
| Fixture-tooling unit | `tests/unit/scripts/` | none (pure functions, `tmp_path`, stubbed BQ client) | none | runs under `mise run test` | `test-pipelines.yml` via `test` |
| dlt integration (live, opt-in) | `tests/integration/dlt/` | DuckDB / live APIs | `live` | `RUN_LIVE_API_TESTS=1 pytest tests/integration -m live` | not run in CI |
| Fixture-tooling live | `tests/integration/scripts/` | DuckDB / live APIs | `live` | `RUN_LIVE_API_TESTS=1 pytest tests/integration -m live` | not run in CI |
| dlt e2e (cloud) | `tests/e2e/dlt/` | Databricks (`live_e2e_test` / `live_data`, `dev_mode=True`) | `e2e` | `mise run test-e2e` (`pytest tests/e2e`) | `test-pipelines.yml` (only with 1Password Environment credentials, skipped for dependabot) |
| dbt transform tests | `dbt/models/**/_*.yml`, `dbt/tests/` | DuckDB (`mock` target) in CI, Databricks otherwise | n/a (`dbt test` / `dbt build`) | `mise run dbt-build-mock`, `mise run dbt-build-dev` | `test-transforms.yml`, `lint.yml` |

> **Note:** the Fitbit dlt pipeline was removed (Fitbit API deprecation), and the
> dbt layer now models Google Health directly: `staging/google_health` models, the
> `stg_google_health__sleep_no_overlapping_sessions` test, and
> `mock_sources/google_health` seeds replace the retired Fitbit artifacts.

## pytest global config

Defined in `pyproject.toml` (`[tool.pytest.ini_options]`) and `tests/conftest.py`:

- `testpaths = ["tests"]`, `python_files = ["test_*.py"]`.
- Markers: `e2e` (cloud Databricks), `live` (calls external APIs, explicit opt-in), `local` (offline).
- `addopts = "-s -p no:pytest-responses --log-cli-level=INFO"`: the `responses` mock library is activated explicitly per-test via the `mock_responses` fixture, so it never interferes with e2e tests.
- Coverage: `source = ["pipelines"]`, branch coverage, reports to `coverage.xml` + `test-results-*.xml` uploaded to Codecov.
- `tests/conftest.py` sets `TEST=True`, `DBT_TARGET=test`, `RUNTIME__LOG_LEVEL=INFO`, and re-wires the `dlt` logger to pytest handlers.

## dlt unit tests (`tests/unit/dlt/`, marker `local`)

Fully offline. `tests/unit/dlt/conftest.py` provides:

- `PROVIDERS__ENABLE_GOOGLE_SECRETS=false`, `DLT_TELEMETRY_DISABLED=1`.
- `mock_responses`: a `responses.RequestsMock` context for HTTP interception.
- `duckdb_pipeline` (class-scoped): both dlt state and the DuckDB file use pytest-managed temporary paths, then the pipeline is dropped after each class.
- Helpers: `resolve_mock_path()` (resolves `tests/fixtures/<source>/` layout), `sample_data()`, `sample_response()` (200 + file body), `sample_resource()` (wraps a JSON fixture in a `@dlt.resource`).

### Per-source pattern: extract / normalize / load + refresh

Each source file (`test_notion_unit.py`, `test_hubspot_unit.py`, `test_google_health_unit.py`) follows the [dlt three phases](https://dlthub.com/docs/reference/explainers/how-dlt-works#the-three-phases):

1. `Test*Phases::test_extract` — mocked API → `pipeline.extract(source)`, asserts one load package.
2. `Test*Phases::test_normalize` — `sample_resource()` from `run1_page1` fixture → `extract()` → `normalize()`, asserts table count and row count (page 1 = 3 rows; HubSpot schemas = 17 rows).
3. `Test*Phases::test_load` — `extract()` → `normalize()` → `load()`, asserts `has_failed_jobs is False` and all packages `loaded`.
4. `test_*_refresh(resource, increment)` (parametrized `True`/`False`) — full `pipeline.run(source)` twice: run 1 loads 5 rows (3 page 1 + 2 page 2); run 2 loads `+1` row when incremental, `+0` (replace) when full refresh.
5. `test_*_pipeline` — all resources together, asserts per-table row counts (e.g. Notion `notion__data_source_daily_habits` = 5, HubSpot contacts/companies/meetings = 5 each).

### Mock strategy per source (`mock_*_apis` fixtures, `responses` callbacks)

- **Notion** (`test_notion_unit.py`): `POST /v1/search` → `notion__data_sources.json`; `POST /v1/data_sources/<id>/query` uses the manifest cursor contract and supports both the normal and `end_date` filter shapes.
- **HubSpot** (`test_hubspot_unit.py`): `POST /crm/v3/objects/<object>/search` validates the `after` cursor against page 1's advertised final id and selects run 2 from the advanced incremental start value, not a date sentinel.
- **Google Health** (`test_google_health_unit.py`): `GET /v4/users/me/dataTypes/{sleep,steps,exercise}/dataPoints` validates `pageToken` against `nextPageToken` and selects run 2 from the changed incremental filter without call-count heuristics.

### Runner / utils / mock-tooling unit tests

- `test_runner_cli.py`: `parse_select()` parsing, `main()` CLI success/unknown-pipeline/exception paths (mocked `refresh_pipeline`), and `refresh_pipeline()` dispatch (source factory args, `with_resources()`, `dlt.pipeline(..., dataset_name=RAW_SCHEMA, destination=<databricks factory with per-schema staging volume>)`, `write_disposition`).
- `test_utils.py`: `should_force_full_refresh()` / `get_refresh_mode()` env-var matrix (`FORCE_FULL_REFRESH`, `<PIPELINE>_FULL_REFRESH`).
- `test_e2e_cleanup.py`: asserts remote e2e schema deletion uses the fully qualified dataset and that local pipeline state is reset even when schema deletion raises.
- `tests/unit/scripts/test_export_mock_responses.py`: pure-function tests for `scripts/fixtures/export_mock_responses.py` — response capture, paginator override (single-page), incremental-limit override, wide export date ranges, dlt-metadata URL matching, cursor sorting, PII scrubbing, 3/2/1 split validation, registry-driven dispatch, and `save_captured_responses()` end-to-end to `tmp_path`.
- `tests/unit/scripts/test_export_mock_seeds.py`: offline tests for `scripts/fixtures/export_mock_seeds.py` with a stubbed BigQuery client — env config, dynamic source discovery from `*_sources.yml`, BigQuery type normalization, scrubbed CSV export, parallel `export_all()` summary, and CLI wiring.

## Fixtures (`tests/fixtures/`)

- **Layout:** `tests/fixtures/<source>/` (`google_health/`, `hubspot/`, `notion/`), resolved by `resolve_mock_path()`.
- **Naming:** `<table>-run1_page1.json` (3 records + pagination cursor), `<table>-run1_page2.json` (2 records, no more data), `<table>-run2.json` (1 new record with an advanced incremental cursor). The contract is declared in `tests/fixtures/manifest.yml` and checked by `test_fixture_manifest.py`; page identities must not overlap and HubSpot's page cursor must equal the final page-1 record id. Non-paginated resources use a single file.
- **Generators (`scripts/fixtures/`):** `export_mock_responses.py` (live capture → sorted, scrubbed, 3/2/1-split fixtures via `process_and_split_payload()`; sources driven by `SOURCE_REGISTRY`, record keys detected dynamically), `scrub_data.py` (dynamic PII scrubbing by key-suffix heuristics + `resolve_project_root()`), `export_mock_seeds.py` (BigQuery raw tables → `dbt/seeds/mock_sources/*.csv`, parallel per-table export, `--source`/`--sample-rows`/`--dry-run` flags).
- **Regeneration is opt-in** (requires real credentials); unit tests only read the checked-in fixtures.

## dlt integration tests (`tests/integration/dlt/`, marker `live`)

Opt-in live API checks loading into local DuckDB (no GCP needed). Skipped
unless `RUN_LIVE_API_TESTS=1`. Shared three-week window
(`2025-06-01` → `2025-06-22`) lives in `tests/test_live_test_range.py` and is
reused by the e2e suite to keep runtimes low:

- `test_notion_integration.py`: full `refresh_pipeline("notion")` → DuckDB, asserts no failed jobs and `notion__data_sources >= 1`.
- `test_hubspot_integration.py`: full `refresh_pipeline("hubspot")` → DuckDB, asserts no failed jobs and `hubspot__schemas >= 1`.
- `test_google_health_integration.py`: full `refresh_pipeline("google_health")` → DuckDB (token via runner), asserts no failed jobs.
- `test_export_mock_responses_live.py` (marker `live`): runs the real HubSpot source through `ResponseCaptureSession` and validates `save_captured_responses()` output structure in `tmp_path`.
- `conftest.py`: class-scoped `duckdb_pipeline` (`live_integration_test` / `live_data`, `pipelines_dir` pointed at a pytest temp dir so no state touches `~/.dlt`) mirroring the unit fixture but without disabling Google Secrets.

## dlt e2e tests (`tests/e2e/dlt/`, marker `e2e`)

Cloud tests against real APIs + Databricks. `mise run test-e2e` starts and
waits for the warehouse first, then runs the tests inside the configured
1Password Environment. `conftest.py` re-enables Google Secrets
(`PROVIDERS__ENABLE_GOOGLE_SECRETS=true`, TOML fragments off, list-secrets on):

- `check_config` (module autouse): asserts `GOOGLE_APPLICATION_CREDENTIALS` exists and the `GoogleSecretsProvider` is configured when `SECRET_STORE=google`.
- `databricks_pipeline` (class-scoped): `dlt.pipeline(pipeline_name="live_e2e_test", destination="databricks", dataset_name="live_data", dev_mode=True)`; `cleanup_databricks_pipeline()` drops the catalog-qualified, dev-mode schema with `DROP SCHEMA IF EXISTS ... CASCADE`, then resets local dlt state even if remote deletion fails.
- `test_load_pipelines.py::TestPipelines`: `REFRESH_ARGS = LIVE_REFRESH_ARGS` (shared three-week window from `tests/test_live_test_range.py`) and one test per pipeline (`notion`, `hubspot`, `google_health`) via `refresh_pipeline()`, asserting `info.has_failed_jobs is False`. Google Health pre-reads its refresh-token secret.

## dbt tests

### Targets (`dbt/profiles.yml`, `DBT_TARGET` env, default `dev`)

| Target | Adapter | Dataset/schema | Source/seeds behaviour (`dbt_project.yml`) |
| ------ | ------- | -------------- | ------------------------------------------ |
| `mock` | DuckDB (`dbt.duckdb`) | `reporting` | sources **disabled**; mock sources/fixtures are used for local model builds |
| `dev` / `test` | Databricks Unity Catalog | catalog `workspace`, schemas `reporting_dev` / `reporting_test` | sources enabled; models read raw schema via `source()` |
| `prod` | Databricks Unity Catalog | catalog `workspace`, schema `reporting` | sources enabled; models read raw schema via `source()` |

Models call `source(source_name, relation_name)` directly. Mock fixtures must
match identifiers in `_*sources.yml`; local DuckDB inputs are wired through
`dbt/test_fixtures/{source}/`.

Other `dbt_project.yml` settings relevant to tests: staging/core = views, intermediate/marts = tables; `vars` (`dbt_date:time_zone`); `warn_error_options.silence` for disabled-source tests under `mock`; packages (`dbt_utils`, `dbt_expectations`, `dbt_date`).

### mise commands

```bash
mise run dbt-deps
mise run dbt-build-mock                         # seed + run + test
mise run dbt-build-mock -- --select "habits*" --full-refresh
mise run dbt-build-dev                          # starts the Databricks SQL warehouse first
mise run dbt -- run --target mock --select stg_notion__daily_habits
mise run dbt -- test --target mock
mise run dbt-lint
mise run dbt-bouncer
```

The dbt project and profiles directories are configured through `mise.toml`'s
`[env]` settings. Mock builds target DuckDB; Databricks builds use catalog
`workspace` and the selected reporting schema.

### Test layers

1. **Schema (generic) data tests** — `data_tests:` blocks in per-domain `_properties.yml` files:
   - `staging/notion/_notion__sources.yml`, `staging/hubspot/_hubspot__sources.yml` (+ `_stg_hubspot__properties.yml` incl. `relationships` tests on association keys), `staging/google_health/_google_health__sources.yml` + `_stg_google_health__properties.yml`, `staging/notion/_stg_notion__properties.yml`, `core/_core__properties.yml`, `intermediate/habits/_int_habits__properties.yml`, `marts/_mrt__properties.yml` (versioned `dim_*_v1`/`fct_*_v1` models: primary-key `not_null`/`unique`, `relationships`, and contract column `data_type`s), `seeds/_seeds__properties.yml`.
2. **Generic tests** — `dbt/tests/generic/`: `expect_column_array_length_to_be_between.sql` (cross-adapter `array_length(json_extract_array())` on BigQuery, `json_array_length()` on DuckDB bounds check) and `expect_intervals_to_not_overlap.sql` (fails on overlapping half-open `[start, end)` intervals via an adjacent-row `LEAD()` check; avoids a DuckDB self-join issue documented in the file header). Applied to the Google Health sleep, steps, and exercise staging models.
3. **Custom generic test** — `dbt/tests/generic/expect_column_array_length_to_be_between.sql`: cross-adapter (`array_length(json_extract_array())` on BigQuery, `json_array_length()` on DuckDB) bounds check.
4. **Contract/structure tests (CI-only, no `dbt test` node)** — `dbt-bouncer` (`dbt/dbt-bouncer.yml`: model/source descriptions populated, model name pattern `^(stg_|int_|core_|fct_|dim_|map_|time_spine_|base_|habits|engagement_contacts)`, full model test/constraint coverage) and `dbt lint` (SQLFluff-compatible engine reusing `dbt/.sqlfluff`: dialect `duckdb`, templater `dbt`, lowercase keywords/identifiers, trailing commas, explicit aliasing).

Mock source coverage lives in `dbt/test_fixtures/{google_health,hubspot,notion}/*.csv` (wired to each source via `config.external_location`); `dbt/seeds/group_connect_cadence.csv` (+ `_seeds__properties.yml`) is shared across all targets.

## CI mapping

- `test-pipelines.yml` (on `pipelines/**`, `tests/**` changes): `mise run setup` → always `mise run test` → `mise run test-e2e` when the 1Password Environment is available → Codecov coverage + test results.
- `test-transforms.yml` (on `dbt/**` changes): `mise run setup` + `mise run dbt-deps` → `mise run dbt-build-mock` → `mise run dbt-build-dev` when 1Password credentials are available.
- `lint.yml` (every PR): `prek` (Python/ruff), `dbt lint` (SQLFluff engine, reuses `dbt/.sqlfluff`, `DBT_TARGET=mock`), `dbt parse` + `dbt compile --write-catalog` (produces `catalog.json` for bouncer; `docs generate` no longer emits one under dbt 2.x), `dbt-bouncer` checks.

Credentialed tasks run inside `op run --environment "$OP_ENVIRONMENT_ID" -- ...`.
The Databricks CLI starts the SQL warehouse and waits for `RUNNING`/`HEALTHY`
before e2e tests and dlt pipeline runs.

## Quick reference

```bash
mise run test                                      # offline dlt unit tests
mise run test-live                                 # opt-in live API tests
mise run test-e2e                                  # live API + Databricks tests
mise run dbt-build-mock                            # local transform check
mise run dbt -- test --target mock --select stg_google_health__sleep
mise run dbt-bouncer                               # manifest checks
mise run pipeline -- notion --full                 # starts warehouse before pipeline
```
