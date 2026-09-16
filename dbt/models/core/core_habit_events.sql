-- ============================================================================
-- CORE LAYER: Habit Events
-- ============================================================================
-- Purpose: Source-agnostic habit event entity. One row per habit entry per
--          target period regardless of which habit tracker recorded it.
--          Tickbox habits carry event_value = 1/0 with is_complete; number
--          habits carry their raw value with is_complete left null — no goal
--          logic lives here, thresholds resolve in the metrics layer from the
--          Notion habit reference table.
--
-- Source: stg_notion__daily_habits, stg_notion__weekly_habits,
--         stg_notion__monthly_habits
-- Output: One row per habit entry per habit
-- ============================================================================

with stg_notion__daily_habits as (

    select * from {{ ref('stg_notion__daily_habits') }}

),

stg_notion__weekly_habits as (

    select * from {{ ref('stg_notion__weekly_habits') }}

),

stg_notion__monthly_habits as (

    select * from {{ ref('stg_notion__monthly_habits') }}

),

-- Transform CTEs: unpivot wide tickbox columns and number columns to events
daily_events as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as event_key,
        page_id as source_id,
        database_id,
        page_name,
        page_date as event_date,
        'day' as period,
        habit,
        'tickbox' as event_type,
        cast(is_complete as integer) as event_value,
        is_complete
    from
        stg_notion__daily_habits
    unpivot (
        is_complete for habit in (
            did_devotional,
            did_journal,
            did_prayer,
            did_read_bible,
            did_workout,
            did_language
        )
    )

),

weekly_events as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as event_key,
        page_id as source_id,
        database_id,
        page_name,
        page_date as event_date,
        'week' as period,
        habit,
        'tickbox' as event_type,
        cast(is_complete as integer) as event_value,
        is_complete
    from
        stg_notion__weekly_habits
    unpivot (
        is_complete for habit in (
            did_fast,
            did_church,
            did_community,
            did_cook,
            did_cardio,
            did_sabbath,
            did_date_night
        )
    )

),

weekly_number_events as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', "'prayer_minutes'"]) }} as event_key,
        page_id as source_id,
        database_id,
        page_name,
        page_date as event_date,
        'week' as period,
        'prayer_minutes' as habit,
        'number' as event_type,
        cast(coalesce(prayer_minutes, 0) as integer) as event_value,
        cast(null as boolean) as is_complete
    from
        stg_notion__weekly_habits

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', "'screen_minutes'"]) }} as event_key,
        page_id as source_id,
        database_id,
        page_name,
        page_date as event_date,
        'week' as period,
        'screen_minutes' as habit,
        'number' as event_type,
        cast(screen_minutes as integer) as event_value,
        cast(null as boolean) as is_complete
    from
        stg_notion__weekly_habits
    where
        screen_minutes is not null

),

monthly_events as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as event_key,
        page_id as source_id,
        database_id,
        page_name,
        page_date as event_date,
        'month' as period,
        habit,
        'tickbox' as event_type,
        cast(is_complete as integer) as event_value,
        is_complete
    from
        stg_notion__monthly_habits
    unpivot (
        is_complete for habit in (
            did_budget,
            did_serve,
            did_travel,
            did_blog,
            did_goal_review,
            did_training
        )
    )

),

combined as (

    select * from daily_events
    union all
    select * from weekly_events
    union all
    select * from weekly_number_events
    union all
    select * from monthly_events

)

select * from combined
