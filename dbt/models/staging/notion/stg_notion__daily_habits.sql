-- ============================================================================
-- STAGING LAYER: Notion Daily Habits
-- ============================================================================
-- Purpose: Clean and standardize raw Notion daily habit data from the
--          database_daily_habits source. Extracts date from JSON, renames
--          columns to snake_case, and deduplicates records.
--
-- Source: notion.database_daily_habits (raw dlt-loaded data)
-- Transformations:
--   - Extract date from JSON date field
--   - Rename columns to standard naming convention
--   - Deduplicate by page_id (keep most recent)
-- Output: One row per unique Notion page with daily habit checkboxes
-- ============================================================================

with daily_habits as (
    -- CTE: Daily Habits Raw Extraction
    -- Purpose: Extract and transform raw Notion data into clean, typed columns.
    --          Handles JSON date extraction and column renaming.
    -- Source: notion.database_daily_habits (raw API data loaded by dlt)
    -- Transformations:
    --   - Extract date from JSON: properties__date__date.start (first 10 chars = YYYY-MM-DD)
    --   - Rename columns: parent__database_id -> database_id, id -> page_id
    --   - Extract checkbox values: properties__{habit}__checkbox -> did_{habit}
    --   - Rename timestamps: created_time -> created_at, last_edited_time -> updated_at

    select
        parent__database_id as database_id,  -- Notion database ID containing this page
        id as page_id,  -- Unique Notion page ID
        -- Extract date from JSON: properties__date__date.start contains ISO date string
        -- Take first 10 characters (YYYY-MM-DD) and cast to date type
        cast(left({{ json_extract_value('properties__date__date', "'$.start'") }}, 10) as date)
          as page_date,
        properties__name__title as page_name,  -- Page title/name
        -- Daily habit checkboxes: true if habit was completed, false otherwise
        properties__devotional__checkbox as did_devotional,  -- Daily devotional
        properties__journal__checkbox as did_journal,  -- Daily journaling
        properties__prayer__checkbox as did_prayer,  -- Daily prayer
        properties__read_bible__checkbox as did_read_bible,  -- Daily Bible reading
        properties__workout__checkbox as did_workout,  -- Daily workout
        properties__language__checkbox as did_language,  -- Daily language learning
        created_time as created_at,  -- When Notion page was created
        last_edited_time as updated_at  -- When Notion page was last edited
        from
            {{ make_source('notion', 'database_daily_habits') }}

),

unique_daily_habits as (
    -- CTE: Deduplicated Daily Habits
    -- Purpose: Remove duplicate records for the same page_id, keeping only
    --          the most recently updated version. This handles incremental
    --          loads where the same page may appear multiple times.
    -- Method: Uses dbt_utils.deduplicate macro with ROW_NUMBER() window function
    --         to identify and keep the latest record per page_id
    -- Output: One row per unique page_id (most recent version)

    {{ dbt_utils.deduplicate(
        relation='daily_habits',
        partition_by='page_id',
        order_by='updated_at desc'
      )
    }}

)

-- Final output: Clean, deduplicated daily habits data
select * from unique_daily_habits
