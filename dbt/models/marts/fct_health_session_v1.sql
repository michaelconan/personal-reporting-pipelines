-- ============================================================================
-- MART LAYER: Health Session Fact
-- ============================================================================
-- Purpose: One row per health session (sleep or exercise), unioning both core
--          entities and disambiguating by session_kind. Sleep flags collapse
--          into sleep_type.
--
-- Sources: core_sleep_sessions, core_exercise_sessions, dim_date
-- Output: One row per sleep or exercise session
-- ============================================================================

with
-- Import CTEs: one per upstream model, selected as-is
core_sleep_sessions as (

    select
        session_id,
        session_date,
        started_at,
        ended_at,
        is_main_sleep,
        is_nap,
        duration_minutes,
        asleep_minutes,
        provider
    from {{ ref('core_sleep_sessions') }}

),

core_exercise_sessions as (

    select
        session_id,
        started_at,
        ended_at,
        exercise_type,
        activity_name,
        active_duration_minutes,
        calories_kcal,
        distance_meters,
        pace_seconds_per_meter,
        step_count,
        provider
    from {{ ref('core_exercise_sessions') }}

),

dim_date as (

    select
        date_key,
        date_day
    from {{ ref('dim_date', v=1) }}

),

-- Transform CTEs: align both session types to a shared column contract
sleep_sessions as (

    select
        'SLEEP' as session_kind,
        cast(session_id as string) as session_id,
        cast(session_date as date) as session_date,
        started_at,
        ended_at,
        case
            when is_main_sleep then 'MAIN'
            when is_nap then 'NAP'
            else 'OTHER'
        end as sleep_type,
        cast(duration_minutes as numeric(38, 9)) as duration_minutes,
        cast(asleep_minutes as bigint) as asleep_minutes,
        cast(null as string) as exercise_type,
        cast(null as string) as activity_name,
        cast(null as numeric(38, 9)) as active_duration_minutes,
        cast(null as bigint) as calories_kcal,
        cast(null as numeric(38, 9)) as distance_meters,
        cast(null as numeric(38, 9)) as pace_seconds_per_meter,
        cast(null as bigint) as step_count,
        provider
    from core_sleep_sessions

),

exercise_sessions as (

    select
        'EXERCISE' as session_kind,
        cast(session_id as string) as session_id,
        cast(started_at as date) as session_date,
        started_at,
        ended_at,
        'NOT_APPLICABLE' as sleep_type,
        cast(active_duration_minutes as numeric(38, 9)) as duration_minutes,
        cast(null as bigint) as asleep_minutes,
        exercise_type,
        activity_name,
        cast(active_duration_minutes as numeric(38, 9)) as active_duration_minutes,
        cast(calories_kcal as bigint) as calories_kcal,
        cast(distance_meters as numeric(38, 9)) as distance_meters,
        cast(pace_seconds_per_meter as numeric(38, 9)) as pace_seconds_per_meter,
        cast(step_count as bigint) as step_count,
        provider
    from core_exercise_sessions

),

sessions as (

    select * from sleep_sessions
    union all
    select * from exercise_sessions

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['s.session_kind', 's.session_id']) }} as session_key,
        coalesce(d.date_key, '1900-01-01') as date_key,
        s.session_kind,
        s.session_id,
        s.sleep_type,
        s.started_at,
        s.ended_at,
        s.duration_minutes,
        s.asleep_minutes,
        s.exercise_type,
        s.activity_name,
        s.active_duration_minutes,
        s.calories_kcal,
        s.distance_meters,
        s.pace_seconds_per_meter,
        s.step_count,
        s.provider
    from sessions as s
    left join dim_date as d on s.session_date = d.date_day

)

select * from final
