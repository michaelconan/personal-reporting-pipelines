-- ============================================================================
-- INTERMEDIATE LAYER: Unified Habits
-- ============================================================================
-- Purpose: Merge and reshape every habit occurrence across source systems into
--          one unified grain for the marts layer:
--          one row per habit source per tracking period.
--
-- Sources: core_habit_events (Notion tickbox + number habits),
--          core_sleep_sessions (sleep minutes), core_daily_steps (steps),
--          stg_hubspot__engagements / stg_hubspot__engagement_contacts
--          (community engagement habits)
-- Transformations:
--   - Classify synchronous engagements as 1-to-1 or group based on contact count
--   - Map every occurrence onto the mart contract
--     (habit_key, source_id, habit_date, habit_period, habit, habit_value)
-- Output: One row per habit occurrence
-- ============================================================================

with core_habit_events as (

    select * from {{ ref('core_habit_events') }}

),

core_sleep_sessions as (

    select * from {{ ref('core_sleep_sessions') }}

),

core_daily_steps as (

    select * from {{ ref('core_daily_steps') }}

),

stg_hubspot__engagements as (

    select
        engagement_id,
        occurred_at,
        is_synchronous
    from {{ ref('stg_hubspot__engagements') }}

),

stg_hubspot__engagement_contacts as (

    select
        engagement_id,
        contact_id
    from {{ ref('stg_hubspot__engagement_contacts') }}

),

habit_events as (

    select
        event_key as habit_key,
        source_id,
        event_date as habit_date,
        period as habit_period,
        habit,
        cast(event_value as numeric(38, 9)) as habit_value
    from
        core_habit_events
    where
        event_value is not null

),

sleep_habits as (
    -- Sleep aggregated to the habit/date grain: multiple sessions on one local
    -- date (main sleep plus naps) collapse to one occurrence with the summed
    -- duration. Threshold comparison happens in the metrics layer against the
    -- Notion habit reference.

    select
        {{ dbt_utils.generate_surrogate_key(['session_date', "'sleep_minutes'"]) }} as habit_key,
        cast(session_date as string) as source_id,
        session_date as habit_date,
        'day' as habit_period,
        'sleep_minutes' as habit,
        cast(sum(duration_minutes) as numeric(38, 9)) as habit_value
    from
        core_sleep_sessions
    group by
        session_date

),

step_habits as (
    -- Daily steps: raw step count; threshold comparison happens in the
    -- metrics layer against the Notion habit reference.

    select
        {{ dbt_utils.generate_surrogate_key(['activity_date', "'steps'"]) }} as habit_key,
        cast(activity_date as string) as source_id,
        activity_date as habit_date,
        'day' as habit_period,
        'steps' as habit,
        cast(step_count as numeric(38, 9)) as habit_value
    from
        core_daily_steps

),

community_meetings as (
    -- Count contacts per synchronous engagement to classify as 1-to-1 or group

    select
        e.engagement_id,
        e.occurred_at,
        count(ec.contact_id) as contact_count
    from
        stg_hubspot__engagements as e
    left join
        stg_hubspot__engagement_contacts as ec
        on e.engagement_id = ec.engagement_id
    where
        e.is_synchronous
    group by
        e.engagement_id,
        e.occurred_at

),

community_habits as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'engagement_id',
            "case when contact_count = 1 then 'met_1to1' else 'met_group' end"
        ]) }} as habit_key,
        cast(engagement_id as string) as source_id,
        cast({{ trunc_date('week', 'cast(occurred_at as date)') }} as date) as habit_date,
        'week' as habit_period,
        case
            when contact_count = 1 then 'met_1to1'
            else 'met_group'
        end as habit,
        cast(1.0 as numeric(38, 9)) as habit_value
    from
        community_meetings

),

combined as (

    select * from habit_events
    union all
    select * from sleep_habits
    union all
    select * from step_habits
    union all
    select * from community_habits

)

select * from combined
