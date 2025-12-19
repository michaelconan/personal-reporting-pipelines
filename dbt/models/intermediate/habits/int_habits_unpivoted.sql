-- ============================================================================
-- INTERMEDIATE LAYER: Habits Unpivoted
-- ============================================================================
-- Purpose: Transform Notion habit data from wide format (one column per habit)
--          to long format (one row per habit). Combines daily, weekly, and
--          monthly habits from separate staging models into a unified structure.
--
-- Transformation: Unpivots checkbox columns (did_devotional, did_journal, etc.)
--                 into rows with habit name and completion status
-- Source: stg_notion__daily_habits, stg_notion__weekly_habits, 
--         stg_notion__monthly_habits
-- Output: One row per page per habit with standardized columns
-- ============================================================================

with daily_habits_unpivoted as (
    -- CTE: Daily Habits Unpivoted
    -- Purpose: Transform daily habits from wide format (columns: did_devotional,
    --          did_journal, etc.) to long format (rows: one per habit).
    -- Source: stg_notion__daily_habits (staging model with checkbox columns)
    -- Transformation: UNPIVOT converts 6 checkbox columns into 6 rows per page
    -- Output: One row per page per daily habit with habit name and completion status

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as row_id,  -- Unique key: page + habit combination
        'day' as habit_period,  -- All daily habits have period = 'day'
        database_id,  -- Notion database ID
        page_id,  -- Notion page ID
        page_date,  -- Date when habits were tracked
        page_name,  -- Name/title of the habit tracking page
        habit,  -- Habit name from unpivot (did_devotional, did_journal, etc.)
        is_complete,  -- Boolean from checkbox value
        created_at,  -- When Notion page was created
        updated_at  -- When Notion page was last edited
    from {{ ref('stg_notion__daily_habits') }}
    unpivot (
        -- UNPIVOT: Convert columns to rows
        -- For each row in source, creates 6 rows (one per habit column)
        is_complete for habit in (
            did_devotional,  -- Daily devotional habit
            did_journal,  -- Daily journaling habit
            did_prayer,  -- Daily prayer habit
            did_read_bible,  -- Daily Bible reading habit
            did_workout,  -- Daily workout habit
            did_language  -- Daily language learning habit
        )
    )

),

weekly_habits_unpivoted as (
    -- CTE: Weekly Habits Unpivoted
    -- Purpose: Transform weekly habits from wide format to long format.
    -- Source: stg_notion__weekly_habits (staging model with checkbox columns)
    -- Transformation: UNPIVOT converts 3 checkbox columns into 3 rows per page
    -- Output: One row per page per weekly habit with habit name and completion status

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as row_id,  -- Unique key: page + habit combination
        'week' as habit_period,  -- All weekly habits have period = 'week'
        database_id,  -- Notion database ID
        page_id,  -- Notion page ID
        page_date,  -- Date when habits were tracked (week start)
        page_name,  -- Name/title of the habit tracking page
        habit,  -- Habit name from unpivot (did_community, did_fast, did_church)
        is_complete,  -- Boolean from checkbox value
        created_at,  -- When Notion page was created
        updated_at  -- When Notion page was last edited
    from {{ ref('stg_notion__weekly_habits') }}
    unpivot (
        -- UNPIVOT: Convert columns to rows
        -- For each row in source, creates 3 rows (one per habit column)
        is_complete for habit in (
            did_community,  -- Weekly community engagement habit
            did_fast,  -- Weekly fasting habit
            did_church  -- Weekly church attendance habit
        )
    )

),

monthly_habits_unpivoted as (
    -- CTE: Monthly Habits Unpivoted
    -- Purpose: Transform monthly habits from wide format to long format.
    -- Source: stg_notion__monthly_habits (staging model with checkbox columns)
    -- Transformation: UNPIVOT converts 4 checkbox columns into 4 rows per page
    -- Output: One row per page per monthly habit with habit name and completion status

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', 'habit']) }} as row_id,  -- Unique key: page + habit combination
        'month' as habit_period,  -- All monthly habits have period = 'month'
        database_id,  -- Notion database ID
        page_id,  -- Notion page ID
        page_date,  -- Date when habits were tracked (month start)
        page_name,  -- Name/title of the habit tracking page
        habit,  -- Habit name from unpivot (did_serve, did_travel, did_budget, did_blog)
        is_complete,  -- Boolean from checkbox value
        created_at,  -- When Notion page was created
        updated_at  -- When Notion page was last edited
    from {{ ref('stg_notion__monthly_habits') }}
    unpivot (
        -- UNPIVOT: Convert columns to rows
        -- For each row in source, creates 4 rows (one per habit column)
        is_complete for habit in (
            did_serve,  -- Monthly service/volunteering habit
            did_travel,  -- Monthly travel habit
            did_budget,  -- Monthly budget review habit
            did_blog  -- Monthly blogging habit
        )
    )

)

-- Final output: Combine all habit periods into unified structure
-- UNION ALL preserves all rows from all three CTEs
select *
from daily_habits_unpivoted
union all
select *
from weekly_habits_unpivoted
union all
select *
from monthly_habits_unpivoted
