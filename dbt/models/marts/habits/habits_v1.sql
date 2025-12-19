-- ============================================================================
-- MART LAYER: Habits
-- ============================================================================
-- Purpose: Unified habits mart combining life habits (Notion), health habits
--          (Fitbit sleep), and community engagement habits (HubSpot) into a
--          single analytics-ready table with consistent structure.
--
-- Grain: One row per habit per date
-- Source: Combines data from int_habits_unpivoted (life habits), 
--         stg_fitbit__sleep (health habits), and engagement_contacts (community habits)
-- ============================================================================

with life_habits as (
    -- CTE: Life Habits from Notion
    -- Purpose: Extract life habits tracked in Notion (devotional, journal, prayer,
    --          bible reading, workout, language learning) from the unpivoted
    --          intermediate model.
    -- Source: int_habits_unpivoted (contains daily, weekly, and monthly habits)
    -- Output: Standardized habit records with habit_key, source_id, date, period,
    --         habit name, and completion status

    select
        row_id as habit_key,  -- Surrogate key from intermediate model
        page_id as source_id,  -- Notion page ID for traceability
        page_date as habit_date,  -- Date when habit was tracked
        habit_period,  -- Period type: 'day', 'week', or 'month'
        habit,  -- Habit name (e.g., 'did_devotional', 'did_journal')
        is_complete  -- Boolean indicating if habit was completed
    from
        {{ ref('int_habits_unpivoted') }}

),

health_habits as (
    -- CTE: Health Habits from Fitbit
    -- Purpose: Transform Fitbit sleep data into habit format, tracking whether
    --          daily sleep goals were met. Sleep goal is defined by the 'sleep_goal'
    --          variable (default: 7 hours = 25,200,000 milliseconds).
    -- Source: stg_fitbit__sleep (staging model for Fitbit sleep logs)
    -- Output: Standardized habit records for sleep goal tracking

    select
        {{ dbt_utils.generate_surrogate_key(['log_id']) }} as habit_key,  -- Generate surrogate key from sleep log ID
        cast(log_id as string) as source_id,  -- Fitbit sleep log ID, cast to string for consistency
        date_of_sleep as habit_date,  -- Date of sleep (not when logged)
        'day' as habit_period,  -- Sleep is tracked daily
        'met_sleep_goal' as habit,  -- Standardized habit name for sleep tracking
        sleep_goal_met as is_complete  -- Boolean: true if sleep duration >= sleep_goal
    from
        {{ ref('stg_fitbit__sleep') }}

),

community_meetings as (
    -- CTE: Community Meeting Aggregations
    -- Purpose: Aggregate synchronous engagements (calls and meetings) to count
    --          the number of contacts per engagement. This enables classification
    --          of engagements as 1-to-1 or group meetings.
    -- Source: engagement_contacts (mart model with engagement-contact grain)
    -- Filter: Only synchronous engagements (is_synchronous = true)
    -- Output: Engagement-level aggregations with contact counts

    select
        engagement_key,  -- Surrogate key for engagement-contact combination
        engagement_id,  -- HubSpot engagement ID
        occurred_at,  -- Timestamp when engagement occurred
        count(contact_id) as contacts  -- Count of contacts involved in the engagement
    from
        {{ ref('engagement_contacts') }}
    where
        is_synchronous  -- Filter to only synchronous engagements (calls/meetings)
    group by
        engagement_key,
        engagement_id,
        occurred_at

),

community_habits as (
    -- CTE: Community Engagement Habits
    -- Purpose: Transform community meeting data into weekly habit format.
    --          Classifies engagements as 1-to-1 or group meetings, aggregates
    --          by week, and determines if weekly meeting goal is met.
    -- Source: community_meetings CTE (aggregated synchronous engagements)
    -- Logic: 
    --   - Habit name: 'met_1to1' for single-contact engagements, 'met_group' for multi-contact
    --   - Completion: true if weekly count >= meet_goal variable (default: 1)
    --   - Date: Truncated to week start for weekly aggregation
    -- Output: Standardized habit records for community engagement tracking

    select
        engagement_key as habit_key,  -- Use engagement key as habit key
        cast(engagement_id as string) as source_id,  -- Engagement ID, cast to string for consistency
        {{ trunc_date('week', 'cast(occurred_at as date)') }} as habit_date,  -- Week start date
        'week' as habit_period,  -- Community habits tracked weekly
        case
            when contacts = 1
            then 'met_1to1'  -- Single-contact engagement (1-to-1 meeting)
            else
                'met_group'  -- Multi-contact engagement (group meeting)
        end as habit,  -- Classify engagement type as habit name
        count(*) >= {{ var('meet_goal') }} as is_complete  -- True if weekly engagement count meets goal
    from
        community_meetings
    group by
        habit_key,
        source_id,
        habit_date,
        habit  -- Group by all non-aggregated columns

)

select *
from life_habits
union all
select *
from health_habits
union all
select *
from community_habits
