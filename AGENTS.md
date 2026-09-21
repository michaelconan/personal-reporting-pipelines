# Personal Reporting Pipelines - AI Agent Context

## Project Overview
Personal data integration and analytics platform tracking personal disciplines/habits across three sources. Uses dlt for extraction into Databricks (Unity Catalog) and dbt for transformation.

## Architecture
```
Notion / HubSpot / Google Health APIs
    ↓ (dlt pipelines → Databricks raw schema / Unity Catalog)
dbt Staging (views) → dbt Intermediate → dbt Marts (tables)
    ↓ MetricFlow semantic layer
```

## Target Configuration
**Primary target**: Databricks Unity Catalog (`dev`, `test`, `prod` targets)
**Rollback targets**: BigQuery (`dev-bigquery`, `test-bigquery`, `prod-bigquery` targets) — retained for easy rollback path
**Local development**: DuckDB (`mock` target) with CSV test fixtures

Default target is `dev` (Databricks). Use `DBT_TARGET` env var to override.

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
- `dbt/seeds/group_connect_cadence.csv` — Target connection cadence per group tier
- `dbt/dbt_project.yml` — dbt config (profile, materializations, vars)
- `dbt/profiles.yml` — dev/test/prod=Databricks, dev-bigquery/test-bigquery/prod-bigquery=BigQuery (rollback), mock=DuckDB

## Source Resolution (no make_source macro)
There is no `make_source` macro. Models call `{{ source(source_name, relation_name) }}` directly for every target:
- **mock (DuckDB)**: each source reads its test fixture via `config.external_location: dbt/test_fixtures/{source}/{identifier}.csv`.
- **dev/test/prod (Databricks)**: sources resolve to the raw schema in Unity Catalog.
- **dev-bigquery/test-bigquery/prod-bigquery (BigQuery)**: sources resolve to the raw schema (rollback path).

**Consequence**: fixtures in `dbt/test_fixtures/{source}/` MUST be named after the source identifiers in `_*sources.yml`.

## Data Model Layers
### Seeds (`dbt/seeds/`)
Reference data for all environments, `snake_case.csv` with a sibling `_seeds__properties.yml`:
- `group_connect_cadence.csv` — target connection cadence per group tier (`cadence_value` + `cadence_period`)

Local mock source data lives in `dbt/test_fixtures/{source}/*.csv` (see Source Resolution above).

### Staging (`dbt/models/staging/`)
- **notion/**: `stg_notion__daily_habits`, `stg_notion__weekly_habits`, `stg_notion__monthly_habits`
- **hubspot/**: `stg_hubspot__contacts`, `stg_hubspot__companies`, `stg_hubspot__engagements`, `stg_hubspot__engagement_contacts`, `stg_hubspot__engagement_companies`; base models: `base_hubspot__engagements`, `base_hubspot__engagement_contacts`
- **google_health/**: `stg_google_health__sleep`, `stg_google_health__steps`, `stg_google_health__exercise`

### Core (`dbt/models/core/`)
Generic, source-agnostic entities that could map to comparable source systems:
- `core_habit_events`: long-format habit events (tickbox + number habits) from Notion staging
- `core_sleep_sessions`: one row per sleep session
- `core_daily_steps`: one row per local activity date with total steps
- `core_exercise_sessions`: one row per exercise session

### Intermediate (`dbt/models/intermediate/habits/`)
- `int_habits`: Merges habit events, sleep minutes, daily steps, and HubSpot engagement habits into one occurrence grain feeding the marts

### Marts (`dbt/models/marts/`)
Conformed dimensions (`dim_*_v1`) and facts (`fct_*_v1`) stored flat in `dbt/models/marts/` (no subdirectories), all with enforced contracts and versioned `_v1` SQL files:
- `dim_date_v1`: Conformed date dimension (calendar spine + attributes, sentinel 1900-01-01)
- `dim_habit_v1`: Habit goal master (targets/thresholds from `stg_notion__habit_reference`, sentinel `UNKNOWN_HABIT`)
- `dim_person_v1`: Contact dimension with group key (sentinel `UNKNOWN_PERSON`)
- `dim_group_v1`: HubSpot company/group dimension (sentinel `UNKNOWN_GROUP`)
- `dim_group_tier_v1`: Group tier cadence reference from the `group_connect_cadence` seed (sentinel `UNKNOWN_TIER`)
- `fct_habit_occurrence_v1`: One row per habit occurrence, completion resolved against `dim_habit`
- `fct_engagement_v1`: One row per engagement-contact association with date/person/group keys
- `fct_health_session_v1`: One row per sleep or exercise session (`session_kind` discriminator)

Model-level and column docs live in `dbt/models/marts/_mrt__properties.yml`.

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

### Google Health
- Sleep table: `google_health__sleep` — `name` (data point id), `sleep__interval__start_time`, `sleep__interval__end_time`, `sleep__summary__minutes_asleep`, `sleep__metadata__{main_sleep,nap}`
- Steps table: `google_health__steps` — `steps__count` (string), `steps__interval__{start_time,end_time}`, civil date columns `steps__interval__civil_start_time__date__{year,month,day}`
- Exercise table: `google_health__exercise` — `name`, `exercise__{exercise_type,display_name,active_duration}`, `exercise__metrics_summary__{steps,calories_kcal,distance_millimeters}`
- Child tables (dlt nested tables): `google_health__sleep__sleep__stages`, `google_health__exercise__exercise__{exercise_events,splits}`

## Habits Data Model

### Habit Keys (values in `habit_key` on `dim_habit` and `fct_habit_occurrence`)
Notion daily (checkboxes): `did_devotional`, `did_journal`, `did_prayer`, `did_read_bible`, `did_workout`, `did_language`
Notion weekly (checkboxes): `did_fast`, `did_church`, `did_community`, `did_sabbath`, `did_cook`, `did_cardio`, `did_date_night`
Notion weekly (numbers): `prayer_minutes`, `screen_minutes`
Notion monthly (checkboxes): `did_budget`, `did_serve`, `did_travel`, `did_blog`, `did_goal_review`, `did_training`
Google Health: `sleep_minutes`, `steps`
HubSpot: `met_1to1`, `met_group`

Note: habit completions and thresholds come from the Notion habit reference data (`stg_notion__habit_reference`) in the metrics layer; goal caps are no longer encoded as dbt vars.

## dbt Variables
- `dbt_date:time_zone`: 'UTC' (used by dbt date utilities)

## Custom Macros
- `json_extract_value(column, path)` — cross-db JSON extraction (BigQuery: `json_extract_scalar`, DuckDB: `json_extract_string`)
- `timestamp_parse(column)` — parse ms timestamps (legacy, no longer needed for new HubSpot model)
- `trunc_date(period, date_expr)` — cross-db date truncation
- `cast_safe(expr, type)` — safe cast
- `seconds_between(start_ts, end_ts)` — cross-db timestamp difference in seconds
- `date_from_parts(year, month, day)` — cross-db date construction
- `date_from_offset_seconds(ts, offset)` — local date from UTC timestamp + seconds offset string
- `unnest_json_array(array_col, alias)` — cross-db JSON array unnesting (legacy)

## Testing
- `make test-local` — Python unit tests
- `make docs` — dbt docs site (v2 SPA) + Sphinx docs
- `uv run dbt build --project-dir dbt --profiles-dir dbt --target mock` — local dbt build (DuckDB + CSV test fixtures)
- `uv run dbt run --project-dir dbt --profiles-dir dbt --select <model> --target mock` — run specific model
- `uv run dbt build --project-dir dbt --profiles-dir dbt --target dev` — build against Databricks dev target
- SQL linting: `uv run dbt lint --project-dir dbt --profiles-dir dbt --target mock` (dbt 2.x built-in linter — rust parser with real dbt templating, reuses `dbt/.sqlfluff` rule config). Verified: full-project run exits 0 (warnings only). The `sqlfluff`/`sqlfluff-templater-dbt` PyPI packages are NOT installed — `sqlfluff-templater-dbt` requires Python `dbt-core` and cannot coexist with the dbt 2.x `dbt` package, and standalone sqlfluff (jinja templater) cannot resolve package macros like `dbt_utils.surrogate_key`, so `dbt lint` is the only reliable lint path.

## dbt 2.x Notes (Rust engine, `dbt~=2.0`)
- dbt-bouncer gets `catalog.json` only from `dbt compile --write-catalog` — `dbt docs generate` no longer writes it.
- `dbt docs generate --static` no longer exists; v2 emits a static SPA (`index.html` + `assets/` + `info_schema/*.parquet`) into `dbt/target/` which `make docs` copies to `docs/_build/html/dbt/`.
- Source-level `freshness` and `meta: external_location` at the top level are rejected by the strict v2 parser (`dbt1060`) — they must live under each source's `config:`. Freshness uses `loaded_at_query` joining `_dlt_loads` on `_dlt_load_id` (`where status = 0`); do not coalesce NULL to `current_timestamp()` (NULL = failed freshness).
- Databricks profile keys `catalog`/`schema` are used for Unity Catalog (interchangeable with `database`/`schema` in other adapters).
- BigQuery profile keys `project`/`dataset` remain valid in the dbt 2.x bigquery adapter (documented as interchangeable with `database`/`schema`).
- Adapters are bundled with the `dbt` distribution (no separate `dbt-duckdb`/`dbt-bigquery`/`dbt-databricks` PyPI packages needed), so `profiles.yml` still uses `type: duckdb` / `type: bigquery` / `type: databricks`.

## Tech Stack
- **Data Ingestion**: dlt (Python)
- **Data Transformation**: dbt 2.x (Rust engine, installed via the `dbt` PyPI distribution)
- **Data Warehouse**: Databricks Unity Catalog (primary), Google BigQuery (rollback)
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
- Dialect: BigQuery (rollback targets), DuckDB (dev), Databricks (primary)
- Keywords: lowercase; identifiers: lowercase with underscores
- Line length: 80; indentation: 4 spaces; trailing commas; explicit aliasing

### dbt model structure (required)
Every SQL model follows this top-to-bottom shape:
1. **Import CTEs at the top** — one CTE per `ref()`/`source()`. This is the only place `ref()` is called, so the model's inputs are visible immediately.
2. **Transform CTEs next** — all joins, casts, derivations, and filtering as named CTEs after the imports.
3. **`final` CTE + `select * from final` last** — the last CTE holds the output shape and the model ends with exactly `select * from final`, keeping column order out of the logic and making debugging easier.

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

Applies to staging, core, intermediate, and marts. Union models align branches in CTEs and union inside `final`.

## Naming Conventions
- **dlt tables**: `{source}__{entity}` (e.g., `hubspot__contacts`)
- **Pipeline functions**: `refresh_{source}()` (e.g., `refresh_hubspot()`)
- **dbt staging**: `stg_{source}__{entity}`; intermediate: `int_{domain}_{description}`; marts: `dim_{entity}_v1` / `fct_{entity}_v1`
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
- `transformations-workflow`
- `rest-api-pipeline-workflow`

## Secret Management
GCP Secret Manager or 1Password (configured via `SECRET_STORE` env var). For local development and CI, Databricks credentials are loaded from 1Password using `op inject` with `.env.databricks.tpl` template.

- **Local**: `make databricks-env-export` (eval output) or `source .env.databricks` after `make inject`
- **CI**: 1Password service account token injects `.env.databricks.tpl` → `.env.databricks` in workflow steps

## Agent Container (multi-devcontainer, `.devcontainer/`)
Least-privilege sandbox for agentic coding (`opencode`, `claude`, `codex`) — the `agent` service in the shared `.devcontainer/docker-compose.yml`, selected via `.devcontainer/agent/devcontainer.json` ("AI Agents"). The full-access human config is `.devcontainer/developer/devcontainer.json` ("Reporting Developer"). Full guide: `docs/source/agent_container.md`.

- **Hardening**: non-root `agent` user (no sudo), `read_only: true`, `cap_drop: [ALL]`, `no-new-privileges`, ephemeral `tmpfs` for `/tmp` and `/home/agent`, no published ports / Docker socket. Image built from `.devcontainer/Dockerfile.agent` (`python:3.13-slim` + git/curl/unzip, Node 22, `openssh-client`, `gh`, `op` 2.32.0, `opencode-ai`/`claude-code`/`codex`, `uv`); tag is Compose-generated (no fixed `image:` name).
- **Build/run**: `cd .devcontainer && docker compose build agent && GITHUB_NAME="..." GITHUB_EMAIL="..." docker compose up -d agent && docker compose exec agent bash`. Needs repo-root `.secrets/opencode_api_key`, `.secrets/github_token`, `.secrets/op_service_account_token` (gitignored).
- **Inside**: entrypoint (`.devcontainer/scripts/entrypoint.sh`) exports `/run/secrets/*` to env (`OPENCODE_API_KEY`/`ANTHROPIC_API_KEY`/`OPENAI_API_KEY`, `GITHUB_TOKEN`, `OP_SERVICE_ACCOUNT_TOKEN`), copies host agent configs only (claude/codex via `.host-*` staging paths; opencode config stays mounted, dlthub skills stay on `~/.agents` mount), sets git identity from `GITHUB_NAME`/`GITHUB_EMAIL` (+ SSH signing only when a usable `SSH_AUTH_SOCK` is provided — Compose forwards none; no keys in image). Repo mounts at `/workspaces/<repo-dir>` (`workspaceFolder: /workspaces/${localWorkspaceFolderBasename}`) and persists; home state is rebuilt each start.
- **Use**: `uv sync` (or `make install`), `make inject` when warehouse creds are needed, then e.g. `opencode run "..."`. Agent wiring: `opencode.json` + `.codex/config.toml` (`dlt-workspace-mcp`), `.agents/skills/`, `AGENTS.md`/`CLAUDE.md`. Validate with `make test-local` and `uv run dbt build --project-dir dbt --profiles-dir dbt --target mock`.

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
