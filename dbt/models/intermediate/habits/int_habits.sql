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

with habit_events as (

    select
        event_key as habit_key,
        source_id,
        event_date as habit_date,
        period as habit_period,
        habit,
        cast(event_value as double) as habit_value
    from
        {{ ref('core_habit_events') }}
    where
        event_value is not null

),

sleep_habits as (
    -- Sleep sessions: raw sleep_minutes; threshold comparison happens in the
    -- metrics layer against the Notion habit reference.

    select
        {{ dbt_utils.generate_surrogate_key(['session_id', "'sleep_minutes'"]) }} as habit_key,
        cast(session_id as varchar) as source_id,
        session_date as habit_date,
        'day' as habit_period,
        'sleep_minutes' as habit,
        cast(duration_minutes as double) as habit_value
    from
        {{ ref('core_sleep_sessions') }}

),

step_habits as (
    -- Daily steps: raw step count; threshold comparison happens in the
    -- metrics layer against the Notion habit reference.

    select
        {{ dbt_utils.generate_surrogate_key(['activity_date', "'steps'"]) }} as habit_key,
        cast(activity_date as varchar) as source_id,
        activity_date as habit_date,
        'day' as habit_period,
        'steps' as habit,
        cast(steps as double) as habit_value
    from
        {{ ref('core_daily_steps') }}

),

community_meetings as (
    -- Count contacts per synchronous engagement to classify as 1-to-1 or group

    select
        e.engagement_id,
        e.occurred_at,
        count(ec.contact_id) as contact_count
    from
        {{ ref('stg_hubspot__engagements') }} as e
    left join
        {{ ref('stg_hubspot__engagement_contacts') }} as ec
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
        cast(engagement_id as varchar) as source_id,
        {{ trunc_date('week', 'cast(occurred_at as date)') }} as habit_date,
        'week' as habit_period,
        case
            when contact_count = 1 then 'met_1to1'
            else 'met_group'
        end as habit,
        1.0 as habit_value
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
