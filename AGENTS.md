# Personal Reporting Pipelines - AI Agent Context

## Project Overview
Personal data integration and analytics platform tracking personal disciplines/habits across three sources. Uses dlt for extraction into BigQuery and dbt for transformation.

## Architecture
```
Notion / HubSpot / Google Health APIs
    ↓ (dlt pipelines → BigQuery raw schema)
dbt Staging (views) → dbt Intermediate → dbt Marts (tables)
    ↓ MetricFlow semantic layer
```

## GitHub Agentic Workflows (gh-aw)
Scheduled AI-driven automation uses [gh-aw](https://github.com/github/gh-aw). Each workflow is a `.md` source file (frontmatter + agent prompt) compiled to a `.lock.yml` GitHub Actions file. Engine for this project: **Gemini** (`GEMINI_API_KEY` secret required).

- **Install extension**: `gh extension install github/gh-aw`
- **Compile after frontmatter edits**: `gh aw compile` (prompt-only edits don't need recompilation)
- **Commit both** the `.md` and the generated `.lock.yml`
- **Run manually**: `gh aw run <workflow-name>`

Current agentic workflow: `weekly-doc-updater` — runs every Monday, opens a PR to keep docs in sync with merged code changes.

## Key Files
- `pipelines/notion.py` — Notion API extraction (data sources, not databases)
- `pipelines/hubspot.py` — HubSpot CRM extraction (per-object: meetings, calls, etc.)
- `pipelines/google_health.py` — Google Health sleep/steps/exercise extraction
- `pipelines/hs_config.yml` — HubSpot object/property config
- `pipelines/__init__.py` — Shared constants: BASE_DATE, RAW_SCHEMA, DBT_SCHEMA
- `Discipline Reference.csv` — Master list of 28 personal habits with targets/thresholds
- `dbt/dbt_project.yml` — dbt config (profile, materializations, vars)
- `dbt/profiles.yml` — dev=duckdb, test/prod=BigQuery

## Critical Macro: make_source
`make_source(source_name, relation_name)` resolves differently by environment:
- **dev (DuckDB)**: `ref('{source_name}__{relation_name}')` → reads from seed files
- **prod/test (BigQuery)**: `source(source_name, relation_name)` → reads from raw schema

**Consequence**: mock seed filenames MUST match the source identifier names in `sources.yml`.

## Data Model Layers
### Seeds (dev only, `dbt/seeds/mock_sources/`)
Mock data for local development. Named `{source}__{table}.csv` to match raw BigQuery table names.

### Seeds (`dbt/seeds/`)
- `discipline_reference.csv` — canonical habit keys, targets, thresholds for all environments

### Staging (`dbt/models/staging/`)
- **notion/**: `stg_notion__daily_habits`, `stg_notion__weekly_habits`, `stg_notion__monthly_habits`
- **hubspot/**: `stg_hubspot__contacts`, `stg_hubspot__companies`, `stg_hubspot__engagements`, `stg_hubspot__engagement_contacts`, `stg_hubspot__engagement_companies`; base models: `base_hubspot__engagements`, `base_hubspot__engagement_contacts`
- **fitbit/**: `stg_fitbit__sleep`, `stg_fitbit__activities`

### Intermediate (`dbt/models/intermediate/habits/`)
- `int_habits_unpivoted`: Unpivots Notion checkbox columns to long format (one row per habit per period)

### Marts (`dbt/models/marts/`)
- `habits/habits_v1`: Unified habits table (Notion checkboxes + Fitbit sleep/steps + HubSpot meetings)
- `habits/habits_metrics_v1`: Completion rates vs. discipline reference targets
- `community/engagement_contacts_v1`: Denormalized engagement-contact-company table

## API Naming Conventions

### Notion (IMPORTANT: uses "data_source" not "database")
- Pipeline function `name_db_table()` generates: `notion__data_source_{name}`
- Tables: `notion__data_source_daily_habits`, `notion__data_source_weekly_habits`, `notion__data_source_monthly_habits`, `notion__data_source_habit_reference`
- Key column: `parent__data_source_id` (NOT `parent__database_id`)
- Date field: `properties__date__date` contains JSON `{"start": "YYYY-MM-DD"}`
- Checkbox fields: `properties__{habit}__checkbox`
- Number fields: `properties__{metric}__number`
- Formula fields: `properties__{metric}__formula` (contains JSON `{"number": value}`)

### HubSpot (IMPORTANT: separate CRM object tables, not single engagements table)
- Per-object tables: `hubspot__meetings`, `hubspot__calls`, `hubspot__communications`, `hubspot__tasks`, `hubspot__notes`
- Per-object properties use prefix: `properties__hs_{object}_{field}` (e.g., `properties__hs_meeting_start_time`)
- Timestamp fields: `created_at`, `updated_at` (already timestamp, not milliseconds)
- Association tables: `hubspot__{object}_to_contacts` with columns:
  - `to_object_id` — contact ID
  - `_hubspot__{object}_id` — parent object ID (from dlt `include_from_parent`)
  - `_hubspot__{object}_updated_at` — parent updatedAt (from dlt `include_from_parent`)

### Fitbit
- Sleep table: `fitbit__sleep` — `log_id`, `date_of_sleep`, `duration` (milliseconds), `start_time`, `end_time`
- Activities table: `fitbit__activities` — `log_id`, `steps`, `start_time`, `last_modified`, `duration`, `active_duration`

## Habits Data Model

### Habit Keys (values in `habit` column of habits mart)
Notion daily (checkboxes): `did_devotional`, `did_journal`, `did_prayer`, `did_read_bible`, `did_workout`, `did_language`
Notion weekly (checkboxes): `did_fast`, `did_church`, `did_community`, `did_sabbath`, `did_cook`, `did_cardio`, `did_date_night`
Notion weekly (numbers): `prayer_minutes` (>=15 goal), `screen_minutes` (<=800 goal)
Notion monthly (checkboxes): `did_budget`, `did_serve`, `did_travel`, `did_blog`, `did_goal_review`, `did_training`
Fitbit: `sleep_minutes` (>=420 goal), `steps` (>=7500 goal)
HubSpot: `met_1to1` (>=2/week goal), `met_group` (>=2/week goal)

## dbt Variables
- `sleep_goal`: 25200000 (7 hours in ms, used in stg_fitbit__sleep)
- `steps_goal`: 7500 (used in stg_fitbit__activities)
- `meet_goal`: 1 (default minimum engagements; discipline_reference has threshold=2)

## Custom Macros
- `make_source(source, relation)` — adapter-aware source/ref resolution
- `json_extract_value(column, path)` — cross-db JSON extraction (BigQuery: `json_extract_scalar`, DuckDB: `json_extract_string`)
- `timestamp_parse(column)` — parse ms timestamps (legacy, no longer needed for new HubSpot model)
- `trunc_date(period, date_expr)` — cross-db date truncation
- `cast_safe(expr, type)` — safe cast
- `unnest_json_array(array_col, alias)` — cross-db JSON array unnesting (legacy)

## Testing
- `make test-local` — Python unit tests
- `make dbt-build target=dev` — local dbt build with DuckDB + mock seeds
- `make dbt-run target=dev select="model"` — run specific model
- SQL linting: `sqlfluff lint --dialect duckdb`

## Tech Stack
- **Data Ingestion**: dlt (Python)
- **Data Transformation**: dbt core
- **Data Warehouse**: Google BigQuery
- **Orchestration**: GitHub Actions
- **Secret Management**: GCP Secret Manager or 1Password (configured via `SECRET_STORE` env var)
- **Development**: Python 3.12, uv, VSCode Dev Containers

## Code Standards

### Python
- **Black** formatting (line length: 100)
- **flake8** linting (max-line-length: 100)
- **mypy** type checking; **bandit** security scanning; **pre-commit** hooks
- Import order: stdlib → PyPI → local
- All pipeline functions accept `is_incremental: Optional[bool] = None`; use `get_refresh_mode()` when `None`

### SQL (SQLFluff)
- Dialect: BigQuery (prod), DuckDB (dev)
- Keywords: lowercase; identifiers: lowercase with underscores
- Line length: 80; indentation: 4 spaces; trailing commas; explicit aliasing

## Naming Conventions
- **dlt tables**: `{source}__{entity}` (e.g., `hubspot__contacts`)
- **Pipeline functions**: `refresh_{source}()` (e.g., `refresh_hubspot()`)
- **dbt staging**: `stg_{source}__{entity}`; intermediate: `int_{domain}_{description}`
- **GitHub Actions workflows**: `{action}-{frequency}` (e.g., `dlt-daily`)
- **Env vars**: `FORCE_FULL_REFRESH` (global), `{PIPELINE_NAME}_FULL_REFRESH` (per-pipeline)

## GitHub Actions Schedule
- **HubSpot**: Daily at 2 AM UTC
- **Google Health**: Mondays at 5 AM UTC
- **Notion**: Weekly on Sundays at 9 AM UTC
- **dbt Transform**: Daily at 4 AM UTC (after ingestion)
- **Weekly doc updater**: Every Monday (gh-aw agentic workflow)


## ALWAYS ACTIVATE those skills
they are essential for ANY work in this project
- `rest-api-pipeline-workflow`

## Security
CRITICAL: never ask for credentials in chat. Always let the user edit secrets directly and do not attempt to read them.

## toolkits — match intent → install → open the entry skill (no discovery round-trip needed)
Workflow toolkits are installed on demand. This index is authoritative for shipped toolkits: match the user's intent, run the install command, confirm from its output (`dlthub ai status` only if unclear), then hand over to the entry skill. No discovery call needed for these.
<!-- This shipped index can drift from the live catalog on a user's machine until runtime refresh lands; tracked in dlt-hub/dlthub-ai-workbench-internal#71. -->

```
intent                                                  → toolkit                | install                                                            | entry skill
ingest from REST / HTTP APIs — production-grade pipeline → rest-api-pipeline     | dlthub --non-interactive ai toolkit install rest-api-pipeline      | find-source
ingest from SQL databases (Postgres, MySQL, Snowflake…) → sql-database-pipeline  | dlthub --non-interactive ai toolkit install sql-database-pipeline  | find-source
load files (CSV/Parquet/JSONL) from disk/S3/GCS/Azure/SFTP → filesystem-pipeline | dlthub --non-interactive ai toolkit install filesystem-pipeline    | create-filesystem-pipeline
explore & profile loaded data, build charts & dashboards → data-exploration      | dlthub --non-interactive ai toolkit install data-exploration       | explore-data
transform & model loaded data (dimensional / Kimball)   → transformations        | dlthub --non-interactive ai toolkit install transformations        | annotate-sources
add data quality checks (column expectations, validation rules) → data-quality   | dlthub --non-interactive ai toolkit install data-quality           | setup-data-quality
deploy / schedule pipelines on the dltHub platform      → dlthub-platform        | dlthub --non-interactive ai toolkit install dlthub-platform        | setup-runtime
guided end-to-end tour, ingest to dashboard (uses the real toolkits) → quick-start | dlthub --non-interactive ai toolkit install quick-start          | quick-start
test/try dlthub end-to-end — minimal pipeline + educational test deploy, NOT production → one-shot       | dlthub --non-interactive ai toolkit install one-shot               | deploy-run-sample-pipeline
build and deploy a minimal custom REST API pipeline after uvx dlthub-init setup → dlthub-init-skills | dlthub --non-interactive ai toolkit install dlthub-init-skills     | deploy-minimal-ingestion-pipeline
optimize / speed up a slow or memory-heavy pipeline — parallelism, workers, batching → performance | dlthub --non-interactive ai toolkit install performance            | optimize-performance
```
* `one-shot` vs `rest-api-pipeline`: one-shot is for **testing / trying dlthub / onboarding / a quick demo** — a minimal single-endpoint, row-limited pipeline on local DuckDB plus an educational test deploy. Educational examples only, NOT production-grade. For a **real or production** REST pipeline (auth, incremental, multiple endpoints, production deploy), use `rest-api-pipeline`. `quick-start` is the guided tour that walks the real toolkits end-to-end.
* Use the `dlthub-router` skill for needs not covered above — it uses live `list_toolkits` to discover newer toolkits.
* DO NOT start data engineering work if no workflow toolkit is installed.

- `init-dlthub-workspace`
