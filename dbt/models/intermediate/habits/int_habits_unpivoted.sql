-- ============================================================================
-- INTERMEDIATE LAYER: Habits Unpivoted
-- ============================================================================
-- Purpose: Transform Notion habit data from wide format (one column per habit)
--          to long format (one row per habit). Combines daily, weekly, and
--          monthly habits from separate staging models into a unified structure.
--
-- Transformation: Unpivots checkbox columns into rows with habit name and
--                 completion status
-- Source: stg_notion__daily_habits, stg_notion__weekly_habits,
--         stg_notion__monthly_habits
-- Output: One row per page per habit with standardized columns
-- ============================================================================

with daily_habits_unpivoted as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as row_id,
        'day' as habit_period,
        database_id,
        page_id,
        page_date,
        page_name,
        habit,
        is_complete,
        created_at,
        updated_at
    from {{ ref('stg_notion__daily_habits') }}
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

weekly_habits_unpivoted as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as row_id,
        'week' as habit_period,
        database_id,
        page_id,
        page_date,
        page_name,
        habit,
        is_complete,
        created_at,
        updated_at
    from {{ ref('stg_notion__weekly_habits') }}
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

monthly_habits_unpivoted as (

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as row_id,
        'month' as habit_period,
        database_id,
        page_id,
        page_date,
        page_name,
        habit,
        is_complete,
        created_at,
        updated_at
    from {{ ref('stg_notion__monthly_habits') }}
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

    select *
    from daily_habits_unpivoted
    union all by name
    select *
    from weekly_habits_unpivoted
    union all by name
    select *
    from monthly_habits_unpivoted

)

select *
from combined
