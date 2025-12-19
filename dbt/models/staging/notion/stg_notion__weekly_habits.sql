-- ============================================================================
-- STAGING LAYER: Notion Weekly Habits
-- ============================================================================
-- Purpose: Clean and standardize raw Notion weekly habit data from the
--          database_weekly_habits source. Extracts date from JSON, extracts
--          numeric values from formula fields, renames columns, and deduplicates.
--
-- Source: notion.database_weekly_habits (raw dlt-loaded data)
-- Transformations:
--   - Extract date from JSON date field
--   - Extract numeric values from formula fields (prayer_minutes)
--   - Rename columns to standard naming convention
--   - Deduplicate by page_id (keep most recent)
-- Output: One row per unique Notion page with weekly habit checkboxes and metrics
-- ============================================================================

with weekly_habits as (
    -- CTE: Weekly Habits Raw Extraction
    -- Purpose: Extract and transform raw Notion data into clean, typed columns.
    --          Handles JSON date extraction, formula field extraction, and column renaming.
    -- Source: notion.database_weekly_habits (raw API data loaded by dlt)
    -- Transformations:
    --   - Extract date from JSON: properties__date__date.start
    --   - Extract numeric from formula: properties__prayer_minutes__formula.number
    --   - Rename columns to standard naming convention
    --   - Extract checkbox and number values

    select
        parent__database_id as database_id,  -- Notion database ID containing this page
        id as page_id,  -- Unique Notion page ID
        -- Extract date from JSON: properties__date__date.start contains ISO date string
        cast(left({{ json_extract_value('properties__date__date', "'$.start'") }}, 10) as date) as page_date,
        properties__name__title as page_name,  -- Page title/name
        -- Weekly habit checkboxes
        properties__fast__checkbox as did_fast,  -- Weekly fasting
        -- Extract numeric value from formula field (calculated field in Notion)
        cast({{ json_extract_value('properties__prayer_minutes__formula', "'$.number'") }} as integer) as prayer_minutes,
        properties__screen_minutes__number as screen_minutes,  -- Screen time in minutes (number field)
        properties__church__checkbox as did_church,  -- Weekly church attendance
        properties__community__checkbox as did_community,  -- Weekly community engagement
        created_time as created_at,  -- When Notion page was created
        last_edited_time as updated_at  -- When Notion page was last edited
    from
        {{ make_source('notion', 'database_weekly_habits') }}

),

unique_weekly_habits as (
    -- CTE: Deduplicated Weekly Habits
    -- Purpose: Remove duplicate records for the same page_id, keeping only
    --          the most recently updated version.
    -- Method: Uses dbt_utils.deduplicate macro with ROW_NUMBER() window function
    -- Output: One row per unique page_id (most recent version)

    {{ dbt_utils.deduplicate(
        relation='weekly_habits',  -- Source CTE to deduplicate
        partition_by='page_id',  -- Group duplicates by page_id
        order_by='updated_at desc',  -- Keep record with latest updated_at
      )
    }}

)

-- Final output: Clean, deduplicated weekly habits data
select * from unique_weekly_habits
