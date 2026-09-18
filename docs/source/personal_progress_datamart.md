# Personal Progress Dimensional Datamart

**Status: specification — the working design is agreed (naming, grains, and open
decisions resolved below); no code changes yet.** Implementation is a separate task.

## Objective

Provide a single, analysis-ready star schema over the merged Notion / HubSpot /
Google Health data that answers three reporting questions:

1. **Habit completion** — how consistently are daily, weekly, and monthly habits
   completed, and how have completion rates trended over time?
2. **Adherence to personal goals** — how often is each habit completed against its
   goal (`target_pct`) and numeric `threshold` from the discipline reference, and
   are goals being met or missed?
3. **Relationship maintenance** — how often are contacts engaged (1:1 meetings,
   group meetings, calls, communications), per contact and per group, against the
   target cadence for each group tier?

## Data flow and scope

```
Sources (dbt staging / core / intermediate — no marts)
    ↓
conformed dimensions                    facts
  dim_date        ┌───────────────────────────────────┐
  dim_habit ←──── fct_habit_occurrence               │
  dim_person ←──┐ fct_engagement                     │
  dim_group ←───┘                                    │
  dim_group_tier ←── (referenced via dim_group)      │
                  fct_health_session ←── dim_date    │
                  └───────────────────────────────────┘
```

The datamart is built **on top of staging, core, and intermediate models, replacing
the current marts**; it does not read raw dlt tables. Inputs:

| Datamart table | Source model(s) | Notes |
|---|---|---|
| `dim_date` | `time_spine_daily` | daily dates; spine extended to start 2016-01-01 |
| `dim_habit` | `stg_notion__habit_reference` | goal master: targets, thresholds, categories |
| `dim_person` | `stg_hubspot__contacts` | contact master |
| `dim_group` | `stg_hubspot__companies` | group / community master |
| `dim_group_tier` | seed `group_connect_cadence` | tier cadence reference (new seed) |
| `fct_habit_occurrence` | `int_habits` | one row per habit occurrence per tracking period |
| `fct_engagement` | `stg_hubspot__engagements`, `stg_hubspot__engagement_contacts`, `stg_hubspot__contacts`, `stg_hubspot__companies` | engagement-contact bridge |
| `fct_health_session` | `core_sleep_sessions`, `core_exercise_sessions` | one row per sleep or exercise session |

**Replaces current marts.** These tables supersede `habits` (v1), `habits_metrics_v1`,
and `engagement_contacts_v1`: the occurrence fact absorbs `habits` v1, the derived
metrics below absorb `habits_metrics_v1`, and `fct_engagement` absorbs
`engagement_contacts_v1`. At implementation time the mart models and their property
files are removed, and the MetricFlow semantic models are repointed at the new facts
(`habits_metrics_v1` logic moves into the derived-metrics layer).

## Habit keys covered

Daily (tickbox): `did_devotional`, `did_journal`, `did_prayer`, `did_read_bible`,
`did_workout`, `did_language`.
Weekly (tickbox): `did_fast`, `did_church`, `did_community`, `did_cook`,
`did_cardio`, `did_sabbath`, `did_date_night`.
Weekly (number): `prayer_minutes`, `screen_minutes`.
Monthly (tickbox): `did_budget`, `did_serve`, `did_travel`, `did_blog`,
`did_goal_review`, `did_training`.
Daily (Google Health): `sleep_minutes`, `steps`.
Weekly (HubSpot count): `met_1to1`, `met_group`.

## Dimensions (5)

Dimension primary keys are **surrogate keys** named with the `_key` suffix (hashed
string via `dbt_utils.generate_surrogate_key`, except where a stable business key is
used directly — see `dim_habit`). Natural source IDs are kept for lineage. Sentinel
rows `UNKNOWN` / `NOT_APPLICABLE` are always created so fact foreign keys are
**never NULL** (Kimball rule #6). Foreign keys are listed immediately after the
primary key.

### `dim_date` — conformed, shared by all facts

Grain: one row per calendar day. Static, no SCD. Built from `time_spine_daily`.

| Column | Type | Notes |
|---|---|---|
| `date_key` | bigint | PK; surrogate, e.g. `to_char(date_day, 'YYYYMMDD')` |
| `date_day` | date | natural key, unique |
| `year`, `quarter`, `month`, `month_name` | int / string | calendar attrs |
| `week_of_year` | int | ISO week number (`dbt_date.iso_week_of_year`) |
| `day_of_week` | int | ISO ordinal 1 (Mon) – 7 (Sun) |
| `is_weekend` | bool | Sat/Sun |

> **Decision:** extend `time_spine_daily` to start **2016-01-01** so pre-2020 Google
> Health rows have a `dim_date` row (no `UNKNOWN_DATE` sentinel needed).

### `dim_habit` — the "goal" master

Grain: one row per habit. **SCD type 1** (see Decisions — target history is not
tracked, by choice).

| Column | Type | Notes |
|---|---|---|
| `habit_key` | PK | string; the canonical business key (`did_*`, `met_*`, snake_case names). It is unique and immutable, so it is used **directly as the surrogate key** — no separate hash |
| `source_id` | string | Notion `page_id` (lineage) |
| `habit_name` | string | e.g. "Workout", "Sleep Minutes" |
| `category` | string | Faith / Life / Health / Community / Work |
| `frequency` | string | day / week / month |
| `habit_type` | string | tickbox / number / count |
| `source` | string | Notion / Hubspot / Google Health |
| `target_pct` | numeric | completion goal (e.g. 0.85 = 85% of periods) |
| `threshold` | numeric | numeric target for number/count habits |
| `is_below_threshold` | bool | invert comparison for `<=` habits |
| `is_active` | bool | whether currently tracked |
| `start_date` | date | goal start |

Carrying `target_pct` / `threshold` in the dimension is what turns the datamart
into a goal-adherence model: completion measures on the fact are compared against
these columns.

### `dim_person` — relationship master

Grain: one row per HubSpot contact. **SCD type 1** (current snapshot).

| Column | Type | Notes |
|---|---|---|
| `person_key` | PK | string; surrogate |
| `group_key` | FK | → `dim_group` (nullable in source → `UNKNOWN`) |
| `contact_id` | bigint | natural key |
| `email` | string | |
| `first_name` / `last_name` | string | |

### `dim_group` — groups and communities

Grain: one row per HubSpot company. HubSpot "company" objects in this dataset
represent personal groups and communities (church small group, meetup, etc.), not
business organisations. **SCD type 1**.

| Column | Type | Notes |
|---|---|---|
| `group_key` | PK | string; surrogate |
| `group_id` | bigint | natural key (HubSpot `company_id`) |
| `group_name` | string | |
| `group_tier` | int | 1–3 parsed from `hs_ideal_customer_profile` |

### `dim_group_tier` — tier cadence reference (new)

Grain: one row per group tier. **SCD type 1**. Built from the new canonical seed
`group_connect_cadence` (`dbt/seeds/group_connect_cadence.csv`, shared across all
targets like the discipline reference).

Proposed seed columns:

`group_tier` (int, 1–3), `cadence_value` (int — target interactions per period),
`cadence_period` (week / month), `description` (optional).

| Column | Type | Notes |
|---|---|---|
| `group_tier_key` | PK | string; surrogate |
| `group_tier` | int | natural key (1–3) |
| `cadence_value` | int | target interactions per `cadence_period` |
| `cadence_period` | string | week / month |

Relationship adherence is evaluated by counting engagements per `dim_group_tier`.
`dim_group` exposes `group_tier` to join it to this dimension.

## Facts (3)

Fact tables use the **`fct_` prefix** and surrogate PKs named `*_key`. Foreign keys
are listed immediately after the primary key.

### `fct_habit_occurrence`

**Grain: one row per habit occurrence per tracking period** (day / week-start /
month-start), matching `int_habits`.

| Column | Type | Notes |
|---|---|---|
| `habit_occurrence_key` | PK | string; surrogate |
| `date_key` | FK | → `dim_date`; `habit_date` (day, week-start, or month-start) |
| `habit_key` | FK | → `dim_habit` |
| `habit_period` | string | degenerate: day / week / month |
| `source_id` | string | lineage: Notion `page_id`, cast date, or engagement id |
| `habit_value` | numeric | measure; 1/0 for tickbox, value for numbers/counts |
| `is_complete` | bool | measure; computed vs `dim_habit` (tickbox `= 1.0`; number `>=`/`<=` threshold per `is_below_threshold`); **NULL for count habits** |

Notes:
- `sleep_minutes` and `steps` appear on this fact as daily habit rows (summed per
  local date), exactly as in `int_habits`. `met_1to1` / `met_group` appear as weekly
  count rows (one row per engagement). See `fct_health_session` / `fct_engagement`
  for the atomic detail behind these aggregates.
- `habit_value` is **additive within a single `habit_period`**; rows at different
  periods must not be summed together (semi-additive across periods).
- Count-habit completeness is not meaningful per row; count adherence is a derived
  metric (`engagement_count >= threshold`), mirroring `habits_metrics_v1`.

### `fct_engagement` — relationship maintenance

**Grain: one row per engagement-contact association** (bridge). An engagement with
`N` contacts produces `N` rows so the fact is analysis-ready per person; engagement-level
attributes are repeated (degenerate) so engagement counts remain possible.

| Column | Type | Notes |
|---|---|---|
| `engagement_key` | PK | string; surrogate per row |
| `date_key` | FK | → `dim_date`; calendar day of `occurred_at` |
| `person_key` | FK | → `dim_person`; `UNKNOWN` for engagements with no contact |
| `group_key` | FK | → `dim_group` via contact; `UNKNOWN` if none |
| `engagement_id` | bigint | degenerate natural key; use `count(distinct ...)` for engagement counts |
| `engagement_type` | string | degenerate: MEETING / CALL / COMMUNICATION |
| `is_synchronous` | bool | degenerate: sync = meeting/call, async = communication |
| `associate_count` | int | measure, engagement-level: contacts on this engagement (repeated per row) |
| `engagement_kind` | string | degenerate: `1to1` (`associate_count = 1`), `group` (`>= 2`), `unspecified` (`0`) |

Notes:
- Deriving `engagement_kind` keeps the fact aligned with the `met_1to1` /
  `met_group` habit classification in `fct_habit_occurrence`.
- `associate_count` is semi-additive (an engagement attribute); engagement counts use
  `count(distinct engagement_id)`.

### `fct_health_session` — sleep and exercise sessions

**Grain: one row per health session.** A union of sleep and exercise sessions,
disambiguated by `session_kind`; kind-specific measures are NULL for the other kind.

| Column | Type | Notes |
|---|---|---|
| `session_key` | PK | string; surrogate |
| `date_key` | FK | → `dim_date`; local date (sleep `session_date`, exercise start) |
| `session_kind` | string | degenerate: SLEEP / EXERCISE |
| `session_id` | string | natural key (`sleep_id` / exercise `name`) |
| `sleep_type` | string | collapsed classification: `MAIN`, `NAP`, `OTHER` for sleep rows (derived from `is_main_sleep` / `is_nap`); `NOT_APPLICABLE` for exercise rows |
| `started_at` / `ended_at` | timestamp | transaction times |
| `duration_minutes` | numeric | measure, present for both kinds |
| `asleep_minutes` | int | measure, sleep only |
| `exercise_type` / `activity_name` | string | degenerate, exercise only |
| `active_duration_minutes` | numeric | measure, exercise only |
| `calories_kcal` / `distance_meters` / `pace_seconds_per_meter` / `step_count` | numeric | measures, exercise only |
| `provider` | string | `google_health` |

Notes:
- `is_main_sleep` / `is_nap` are collapsed into `sleep_type` (precedence `MAIN` over
  `NAP`). The source's lower-level `sleep__type` (CLASSIC / STAGES) is not carried
  into the fact.
- Daily totals (`sleep_minutes`, `steps`) already exist at the daily grain in
  `fct_habit_occurrence`; this fact exposes the **session-level detail** (naps,
  calories, distance, pace). The redundancy is intentional (two grains, two questions).
- A separate `fct_daily_steps` may be split out later if per-interval/device step
  analysis is wanted (`stg_google_health__steps` holds `device_name`,
  `data_source_platform`, interval counts).

## New seed: `group_connect_cadence`

Canonical seed at `dbt/seeds/group_connect_cadence.csv` (dev via DuckDB `ref`,
prod via BigQuery), feeding `dim_group_tier`. Columns: `group_tier` (1–3),
`cadence_value` (int), `cadence_period` (week / month), `description` (optional).

## Derived metrics (reporting layer, not new tables)

| Theme | Metric | Computed from |
|---|---|---|
| Habit completion | completion_rate = `sum(is_complete) / count(*)` per habit per week/month/year | `fct_habit_occurrence` + `dim_date` |
| Habit completion | streak of consecutive complete weeks/months | `fct_habit_occurrence` |
| Adherence | `target_met` = completion_rate `>= dim_habit.target_pct` | completion_rate + `dim_habit` |
| Adherence | gap = `target_pct - completion_rate`; % of active goals met | above |
| Adherence | below-threshold alert for number habits (`is_below_threshold`) | `fct_habit_occurrence` + `dim_habit` |
| Relationship | 1:1 / group / synchronous / async engagements per week | `fct_engagement` (distinct `engagement_id`) |
| Relationship | distinct contacts met per month; contacts by group or tier | `fct_engagement` + `dim_person`/`dim_group` |
| Relationship | tier cadence adherence = engagements per tier per `cadence_period` vs `cadence_value` | `fct_engagement` + `dim_group` + `dim_group_tier` |
| Health | avg sleep minutes per night; naps share; exercise sessions / calories / distance / steps per week | `fct_health_session`, `fct_habit_occurrence` |

These map directly onto the three goals: `habit completion` and `adherence` rows come
from the habit fact, `relationship` rows from the engagement fact (including tier
cadence adherence), and health habits (`sleep_minutes`, `steps`) are native habit
rows in the habit fact with detail in the health-session fact.

## Conventions and placement (implementation time)

- Proposed location: `dbt/models/marts/datamart/`; files `{model}_v1.sql` following
  the repo's `{entity}_v1` file convention. Model names: `dim_date`, `dim_habit`,
  `dim_person`, `dim_group`, `dim_group_tier`, `fct_habit_occurrence`,
  `fct_engagement`, `fct_health_session`. The `dim_`/`fct_` prefixes already satisfy
  the dbt-bouncer model-name pattern `^(stg_|int_|core_|fct_|dim_|...)`.
- Surrogate keys use the `_key` suffix; foreign keys follow the PK in each table.
- Conformed `dim_date` must be identical (same surrogate scheme) across all facts.
- Properties in `_mrt_datamart__properties.yml` with enforced contracts and PK
  constraints (surrogate keys).
- On implementation, remove `habits_v1.sql`, `habits_metrics_v1.sql`,
  `engagement_contacts_v1.sql` and their property files; repoint MetricFlow semantic
  models to the new facts.

## Decisions and risks (resolved)

1. **Goal-target history.** `dim_habit` is SCD type 1 by choice — the habit-reference
   source only retains the current snapshot, and historical target changes are not
   required. Adherence is measured against current targets.
2. **Time spine range.** Extend `time_spine_daily` to start 2016-01-01 so all Google
   Health history is covered by `dim_date`.
3. **Engagement grain.** Bridge grain only (per engagement-contact); no separate
   engagement-grain fact for now. Engagement counts use `count(distinct engagement_id)`.
4. **`is_complete` semantics.** NULL for count habits by design; count adherence is
   computed at aggregation. Dashboards must not treat NULL as failure.
5. **Non-synchronous communications** are excluded from the `met_*` habit counts
   (`int_habits` keeps only meetings/calls) but ARE present in `fct_engagement`, so
   channel-level relationship reporting is possible while habit adherence follows the
   existing definition.
6. **`dim_habit` surrogate.** The canonical `habit_key` doubles as the dimension
   surrogate (unique and immutable); no separate hash is generated.

## Subsequent implementation steps

When approved, the work follows the `transformations` workflow toolkit
(`annotate-sources` → `create-ontology` → `generate-cdm` → `create-transformation`),
producing the eight tables above as dbt models under `marts/datamart/`, the
`group_connect_cadence` seed, removal of the three superseded mart models, plus
optional MetricFlow metrics on completion rate and adherence.