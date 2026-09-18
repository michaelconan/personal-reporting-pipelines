# Tests

This repo has two test systems: **pytest** for `pipelines/` (dlt) and **dbt built-in tests** for `dbt/` transformations. CI runs both plus SQL/Python linting.

| Layer | Location | Destination | pytest marker | Local command | CI workflow |
| ----- | -------- | ----------- | ------------- | ------------- | ----------- |
| dlt unit (offline) | `tests/unit/dlt/` | DuckDB (`local_unit_test` / `local_data`, `dev_mode=True`) | `local` | `make test-local` (`pytest tests/unit`) | `test-pipelines.yml` (always runs) |
| Fixture-tooling unit | `tests/unit/scripts/` | none (pure functions, `tmp_path`, stubbed BQ client) | none | runs under `make test-local` | `test-pipelines.yml` via `test-local` |
| dlt integration (live, opt-in) | `tests/integration/dlt/` | DuckDB / live APIs | `live` | `RUN_LIVE_API_TESTS=1 pytest tests/integration -m live` | not run in CI |
| Fixture-tooling live | `tests/integration/scripts/` | DuckDB / live APIs | `live` | `RUN_LIVE_API_TESTS=1 pytest tests/integration -m live` | not run in CI |
| dlt e2e (cloud) | `tests/e2e/dlt/` | BigQuery (`live_e2e_test` / `live_data`, `dev_mode=True`) | `e2e` | `make test-e2e` (`pytest tests/e2e`) | `test-pipelines.yml` (only with secrets, skipped for dependabot) |
| dbt transform tests | `dbt/models/**/_*.yml`, `dbt/tests/` | DuckDB (`mock` target) in CI, BigQuery otherwise | n/a (`dbt test` / `dbt build`) | `make dbt-build target=mock`, `make dbt-test target=mock` | `test-transforms.yml`, `lint.yml` |

> **Note:** the Fitbit dlt pipeline was removed (Fitbit API deprecation), and the
> dbt layer now models Google Health directly: `staging/google_health` models, the
> `stg_google_health__sleep_no_overlapping_sessions` test, and
> `mock_sources/google_health` seeds replace the retired Fitbit artifacts.

## pytest global config

Defined in `pyproject.toml` (`[tool.pytest.ini_options]`) and `tests/conftest.py`:

- `testpaths = ["tests"]`, `python_files = ["test_*.py"]`.
- Markers: `e2e` (cloud BigQuery), `live` (calls external APIs, explicit opt-in), `local` (offline).
- `addopts = "-s -p no:pytest-responses --log-cli-level=INFO"`: the `responses` mock library is activated explicitly per-test via the `mock_responses` fixture, so it never interferes with e2e tests.
- Coverage: `source = ["pipelines"]`, branch coverage, reports to `coverage.xml` + `test-results-*.xml` uploaded to Codecov.
- `tests/conftest.py` sets `TEST=True`, `DBT_TARGET=test`, `RUNTIME__LOG_LEVEL=INFO`, and re-wires the `dlt` logger to pytest handlers.

## dlt unit tests (`tests/unit/dlt/`, marker `local`)

Fully offline. `tests/unit/dlt/conftest.py` provides:

- `PROVIDERS__ENABLE_GOOGLE_SECRETS=false`, `DLT_TELEMETRY_DISABLED=1`.
- `mock_responses`: a `responses.RequestsMock` context for HTTP interception.
- `duckdb_pipeline` (class-scoped): `pipelines_dir` pointed at a pytest temp dir (no state touches `~/.dlt`), `dlt.pipeline(pipeline_name="local_unit_test", destination="duckdb", dataset_name="local_data", dev_mode=True)`, dropped after each class.
- Helpers: `resolve_mock_path()` (resolves `tests/fixtures/<source>/` layout), `sample_data()`, `sample_response()` (200 + file body), `sample_resource()` (wraps a JSON fixture in a `@dlt.resource`).

### Per-source pattern: extract / normalize / load + refresh

Each source file (`test_notion_unit.py`, `test_hubspot_unit.py`, `test_google_health_unit.py`) follows the [dlt three phases](https://dlthub.com/docs/reference/explainers/how-dlt-works#the-three-phases):

1. `Test*Phases::test_extract` — mocked API → `pipeline.extract(source)`, asserts one load package.
2. `Test*Phases::test_normalize` — `sample_resource()` from `run1_page1` fixture → `extract()` → `normalize()`, asserts table count and row count (page 1 = 3 rows; HubSpot schemas = 17 rows).
3. `Test*Phases::test_load` — `extract()` → `normalize()` → `load()`, asserts `has_failed_jobs is False` and all packages `loaded`.
4. `test_*_refresh(resource, increment)` (parametrized `True`/`False`) — full `pipeline.run(source)` twice: run 1 loads 5 rows (3 page 1 + 2 page 2); run 2 loads `+1` row when incremental, `+0` (replace) when full refresh.
5. `test_*_pipeline` — all resources together, asserts per-table row counts (e.g. Notion `notion__data_source_daily_habits` = 5, HubSpot contacts/companies/meetings = 5 each).

### Mock strategy per source (`mock_*_apis` fixtures, `responses` callbacks)

- **Notion** (`test_notion_unit.py`): `POST /v1/search` → `notion__data_sources.json`; `POST /v1/data_sources/<id>/query` cursor pagination (`start_cursor`, `last_edited_time.after` filter) → `notion__data_source_rows-run1_page1.json` (3) / `-run1_page2.json` (2) / `-run2.json` (1, detected by `after` date `2024-06-05`).
- **HubSpot** (`test_hubspot_unit.py`): `POST /crm/v3/objects/<object>/search` for 9 `CRM_OBJECTS` (contacts, companies, meetings, calls, communications, tasks, notes, deals, tickets) → per-object `-run1_page1` (3) / `-run1_page2` (2) / `-run2` (1, detected by `GTE` filter value); `GET /crm/v4/objects/.../associations/...` stubbed to `{"results": []}` for full-pipeline runs only; `GET /crm-object-schemas/...` → `hubspot_schemas_contacts.json`. Missing fixture files fall back to `{"total": 0, "results": []}`.
- **Google Health** (`test_google_health_unit.py`): `GET /v4/users/me/dataTypes/{sleep,steps,exercise}/dataPoints`, `pageToken` → page 2, `filter` ISO date (non-`1970-01-01`) → run 2, else page 1; call-count fallback (≥4 calls) → run 2.

### Runner / utils / mock-tooling unit tests

- `test_runner_cli.py`: `parse_select()` parsing, `main()` CLI success/unknown-pipeline/exception paths (mocked `refresh_pipeline`), and `refresh_pipeline()` dispatch (source factory args, `with_resources()`, `dlt.pipeline(..., dataset_name=RAW_SCHEMA, destination="bigquery")`, `write_disposition`).
- `test_utils.py`: `should_force_full_refresh()` / `get_refresh_mode()` env-var matrix (`FORCE_FULL_REFRESH`, `<PIPELINE>_FULL_REFRESH`).
- `tests/unit/scripts/test_export_mock_responses.py`: pure-function tests for `scripts/fixtures/export_mock_responses.py` — response capture, paginator override (single-page), incremental-limit override, wide export date ranges, dlt-metadata URL matching, cursor sorting, PII scrubbing, 3/2/1 split validation, registry-driven dispatch, and `save_captured_responses()` end-to-end to `tmp_path`.
- `tests/unit/scripts/test_export_mock_seeds.py`: offline tests for `scripts/fixtures/export_mock_seeds.py` with a stubbed BigQuery client — env config, dynamic source discovery from `*_sources.yml`, BigQuery type normalization, scrubbed CSV export, parallel `export_all()` summary, and CLI wiring.

## Fixtures (`tests/fixtures/`)

- **Layout:** `tests/fixtures/<source>/` (`google_health/`, `hubspot/`, `notion/`), resolved by `resolve_mock_path()`.
- **Naming:** `<table>-run1_page1.json` (3 records + pagination cursor), `<table>-run1_page2.json` (2 records, no more data), `<table>-run2.json` (1 record with a larger cursor value, e.g. date) — exactly the convention described in the original `TESTS.md` intro. Non-paginated resources (e.g. `notion__data_sources.json`, `hubspot_schemas_contacts.json`, `hubspot__*_to_contacts.json`) are single files.
- **Generators (`scripts/fixtures/`):** `export_mock_responses.py` (live capture → sorted, scrubbed, 3/2/1-split fixtures via `process_and_split_payload()`; sources driven by `SOURCE_REGISTRY`, record keys detected dynamically), `scrub_data.py` (dynamic PII scrubbing by key-suffix heuristics + `resolve_project_root()`), `export_mock_seeds.py` (BigQuery raw tables → `dbt/seeds/mock_sources/*.csv`, parallel per-table export, `--source`/`--sample-rows`/`--dry-run` flags).
- **Regeneration is opt-in** (requires real credentials); unit tests only read the checked-in fixtures.

## dlt integration tests (`tests/integration/dlt/`, marker `live`)

Opt-in live API checks loading into local DuckDB (no GCP needed). Skipped
unless `RUN_LIVE_API_TESTS=1`. Shared narrow 7-day window
(`2025-06-01` → `2025-06-08`) lives in `tests/live_test_range.py` and is
reused by the e2e suite to keep runtimes low:

- `test_notion_integration.py`: full `refresh_pipeline("notion")` → DuckDB, asserts no failed jobs and `notion__data_sources >= 1`.
- `test_hubspot_integration.py`: full `refresh_pipeline("hubspot")` → DuckDB, asserts no failed jobs and `hubspot__schemas >= 1`.
- `test_google_health_integration.py`: full `refresh_pipeline("google_health")` → DuckDB (token via runner), asserts no failed jobs.
- `test_export_mock_responses_live.py` (marker `live`): runs the real HubSpot source through `ResponseCaptureSession` and validates `save_captured_responses()` output structure in `tmp_path`.
- `conftest.py`: class-scoped `duckdb_pipeline` (`live_integration_test` / `live_data`, `pipelines_dir` pointed at a pytest temp dir so no state touches `~/.dlt`) mirroring the unit fixture but without disabling Google Secrets.

## dlt e2e tests (`tests/e2e/dlt/`, marker `e2e`)

Cloud tests against real APIs + BigQuery. `conftest.py` re-enables Google Secrets (`PROVIDERS__ENABLE_GOOGLE_SECRETS=true`, TOML fragments off, list-secrets on):

- `check_config` (module autouse): asserts `GOOGLE_APPLICATION_CREDENTIALS` exists and the `GoogleSecretsProvider` is configured when `SECRET_STORE=google`.
- `bigquery_pipeline` (class-scoped): `dlt.pipeline(pipeline_name="live_e2e_test", destination="bigquery", dataset_name="live_data", dev_mode=True)`; drops pipeline + dataset schema afterwards.
- `test_load_pipelines.py::TestPipelines`: `REFRESH_ARGS = LIVE_REFRESH_ARGS` (narrow 7-day window from `tests/live_test_range.py`, shared with integration tests) and one test per pipeline (`notion`, `hubspot`, `google_health`) via `refresh_pipeline()`, asserting `info.has_failed_jobs is False`. Google Health pre-reads its refresh-token secret.

## dbt tests

### Targets (`dbt/profiles.yml`, `DBT_TARGET` env, default `dev`)

| Target | Adapter | Dataset/schema | Source/seeds behaviour (`dbt_project.yml`) |
| ------ | ------- | -------------- | ------------------------------------------ |
| `mock` | DuckDB (`dbt.duckdb`) | `reporting` | sources **disabled** (`enabled: "{{ target.name != 'mock' }}"`); `mock_sources` seeds **enabled**, schema `mock`; models read seeds via `make_source()` → `ref()` |
| `dev` / `test` | BigQuery (service account) | `dev_reporting` / `test_reporting` | sources enabled; models read raw schema via `make_source()` → `source()` |
| `prod` | BigQuery | `reporting` | same as dev/test |

`make_source(source_name, relation_name)` is adapter-aware, so **mock seed filenames must match raw table names** (`{source}__{table}.csv` in `dbt/seeds/mock_sources/{notion,hubspot,google_health}/`). Key consequence, not just convention: a renamed raw table breaks `mock` builds.

Other `dbt_project.yml` settings relevant to tests: staging/core = views, intermediate/marts = tables; `vars` (`dbt_date:time_zone`); `warn_error_options.silence` for disabled-source tests under `mock`; packages (`dbt_utils`, `dbt_expectations`, `dbt_date`).

### Commands (`Makefile`, `DBTARGS = --project-dir dbt --profiles-dir dbt`)

```bash
make dbt-seed target=mock        # seeds only (excludes source:* on mock)
make dbt-run target=mock select="stg_notion__daily_habits"
make dbt-test target=mock        # dbt tests only
make dbt-build target=mock       # seed + run + test (what CI runs)
make dbt-build target=mock select="habits*" full=true  # full-refresh variant
make dbt-docs target=mock
make dbt-bouncer                  # manifest checks (dbt/dbt-bouncer.yml)
```

`DBT_EXCLUDE = --exclude "source:*"` applies automatically when `target=mock`.

### Test layers

1. **Schema (generic) data tests** — `data_tests:` blocks in per-domain `_properties.yml` files:
   - `staging/notion/_notion__sources.yml`, `staging/hubspot/_hubspot__sources.yml` (+ `_stg_hubspot__properties.yml` incl. `relationships` tests on association keys), `staging/google_health/_google_health__sources.yml` + `_stg_google_health__properties.yml`, `staging/notion/_stg_notion__properties.yml`, `core/_core__properties.yml`, `intermediate/habits/_int_habits__properties.yml`, `marts/habits/_mrt_habits__properties.yml` (`not_null`, `unique`, `accepted_values` on habit keys), `marts/community/_mrt_community__properties.yml` (`dbt_expectations.expect_compound_columns_to_be_unique` + `not_null`/`unique`).
2. **Generic tests** — `dbt/tests/generic/`: `expect_column_array_length_to_be_between.sql` (cross-adapter `array_length(json_extract_array())` on BigQuery, `json_array_length()` on DuckDB bounds check) and `expect_intervals_to_not_overlap.sql` (fails on overlapping half-open `[start, end)` intervals via an adjacent-row `LEAD()` check; avoids a DuckDB self-join issue documented in the file header). Applied to the Google Health sleep, steps, and exercise staging models.
3. **Custom generic test** — `dbt/tests/generic/expect_column_array_length_to_be_between.sql`: cross-adapter (`array_length(json_extract_array())` on BigQuery, `json_array_length()` on DuckDB) bounds check.
4. **Contract/structure tests (CI-only, no `dbt test` node)** — `dbt-bouncer` (`dbt/dbt-bouncer.yml`: model/source descriptions populated, model name pattern `^(stg_|int_|core_|fct_|dim_|map_|time_spine_|base_|habits|engagement_contacts)`, ≥70% model test coverage) and `dbt lint` (SQLFluff-compatible engine reusing `dbt/.sqlfluff`: dialect `duckdb`, templater `dbt`, lowercase keywords/identifiers, trailing commas, explicit aliasing).

Mock seed coverage lives in `dbt/seeds/mock_sources/{google_health,hubspot,notion}/*.csv` (+ `_properties.yml`); canonical `discipline_reference.csv` seed is shared across all targets.

## CI mapping

- `test-pipelines.yml` (on `pipelines/**`, `tests/**` changes): `make install` → `make inject` (1Password, skipped for dependabot) → always `make test-local` → `make test-e2e` when secrets exist → Codecov coverage (`coverage.xml`) + test results (`test-results-*.xml`).
- `test-transforms.yml` (on `dbt/**` changes): `make install` + `make dbt-deps` → `make dbt-build target=mock` (seed+run+test) → `make dbt-docs target=mock`.
- `lint.yml` (every PR): `prek` (Python/ruff), `dbt lint` (SQLFluff engine, reuses `dbt/.sqlfluff`, `DBT_TARGET=mock`), `dbt parse` + `dbt compile --write-catalog` (produces `catalog.json` for bouncer; `docs generate` no longer emits one under dbt 2.x), `dbt-bouncer` checks.

## Quick reference

```bash
make test-local                  # offline dlt unit tests (DuckDB + fixtures)
make test-e2e                    # cloud dlt e2e tests (needs GCP + API secrets)
RUN_LIVE_API_TESTS=1 uv run pytest tests/integration -m live -v -s  # opt-in live checks
make dbt-build target=mock       # full local transform check (seed + run + test)
make dbt-test target=mock select="stg_google_health__sleep"  # single-model dbt tests
uv run dbt-bouncer --config-file dbt/dbt-bouncer.yml  # or: make dbt-bouncer
```
