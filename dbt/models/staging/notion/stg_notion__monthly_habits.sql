-- ============================================================================
-- STAGING LAYER: Notion Monthly Habits
-- ============================================================================
-- Purpose: Clean and standardize raw Notion monthly habit data from the
--          database_monthly_habits source. Extracts date from JSON, renames
--          columns to snake_case, and deduplicates records.
--
-- Source: notion.database_monthly_habits (raw dlt-loaded data)
-- Transformations:
--   - Extract date from JSON date field
--   - Rename columns to standard naming convention
--   - Deduplicate by page_id (keep most recent)
-- Output: One row per unique Notion page with monthly habit checkboxes
-- ============================================================================

with monthly_habits as (
    -- CTE: Monthly Habits Raw Extraction
    -- Purpose: Extract and transform raw Notion data into clean, typed columns.
    --          Handles JSON date extraction and column renaming.
    -- Source: notion.database_monthly_habits (raw API data loaded by dlt)
    -- Transformations:
    --   - Extract date from JSON: properties__date__date.start
    --   - Rename columns: parent__database_id -> database_id, id -> page_id
    --   - Extract checkbox values: properties__{habit}__checkbox -> did_{habit}

    select
        parent__database_id as database_id,  -- Notion database ID containing this page
        id as page_id,  -- Unique Notion page ID
        -- Extract date from JSON: properties__date__date.start contains ISO date string
        -- Take first 10 characters (YYYY-MM-DD) and cast to date type
        cast(left({{ json_extract_value('properties__date__date', "'$.start'") }}, 10) as date)
          as page_date,
        properties__name__title as page_name,  -- Page title/name
        -- Monthly habit checkboxes: true if habit was completed, false otherwise
        properties__budget__checkbox as did_budget,  -- Monthly budget review
        properties__serve__checkbox as did_serve,  -- Monthly service/volunteering
        properties__travel__checkbox as did_travel,  -- Monthly travel
        properties__blog__checkbox as did_blog,  -- Monthly blogging
        created_time as created_at,  -- When Notion page was created
        last_edited_time as updated_at  -- When Notion page was last edited
    from
        {{ make_source('notion', 'database_monthly_habits') }}

),

unique_monthly_habits as (
    -- CTE: Deduplicated Monthly Habits
    -- Purpose: Remove duplicate records for the same page_id, keeping only
    --          the most recently updated version.
    -- Method: Uses dbt_utils.deduplicate macro with ROW_NUMBER() window function
    -- Output: One row per unique page_id (most recent version)

    {{ dbt_utils.deduplicate(
        relation='monthly_habits',
        partition_by='page_id',
        order_by='updated_at desc'
      )
    }}

)

-- Final output: Clean, deduplicated monthly habits data
select * from unique_monthly_habits
