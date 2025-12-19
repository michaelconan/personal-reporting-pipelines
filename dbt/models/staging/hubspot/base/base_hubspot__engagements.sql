-- ============================================================================
-- STAGING LAYER (BASE): HubSpot Engagements Base
-- ============================================================================
-- Purpose: Base model for HubSpot engagements that handles deduplication
--          before creating junction tables. Extracts engagement data, parses
--          timestamps from milliseconds, and preserves association arrays.
--
-- Source: hubspot.engagements (raw dlt-loaded data)
-- Transformations:
--   - Rename columns to standard naming convention
--   - Parse timestamps from millisecond values using adapter-aware macro
--   - Preserve JSON arrays for company_ids and contact_ids (for junction tables)
--   - Deduplicate by engagement_id (keep most recent)
-- Output: One row per unique engagement with association arrays intact
-- Note: This is a base model used by stg_hubspot__engagements and junction tables
-- ============================================================================

with engagements as (
    -- CTE: Engagements Raw Extraction
    -- Purpose: Extract and transform raw HubSpot engagement data into clean, typed columns.
    --          Handles timestamp parsing and preserves association arrays for junction tables.
    -- Source: hubspot.engagements (raw API data loaded by dlt)
    -- Transformations:
    --   - Rename columns: engagement__id -> engagement_id, etc.
    --   - Parse timestamps: Convert millisecond values to timestamp using adapter-aware macro
    --   - Preserve arrays: Keep company_ids and contact_ids as JSON arrays for unnesting

    select
        engagement__id as engagement_id,  -- HubSpot engagement ID
        engagement__type as engagement_type,  -- Type of engagement (EMAIL, CALL, MEETING, etc.)
        engagement__body_preview as body_preview,  -- Preview text of engagement content
        associations__company_ids as company_ids,  -- JSON array of associated company IDs
        associations__contact_ids as contact_ids,  -- JSON array of associated contact IDs
        engagement__last_updated as updated_at,  -- When engagement was last updated (milliseconds)
        -- Parse timestamp from milliseconds (adapter-aware: BigQuery uses timestamp_millis,
        -- DuckDB uses make_timestamp)
        {{ timestamp_parse('engagement__timestamp') }} as occurred_at,  -- When engagement occurred
        {{ timestamp_parse('engagement__created_at') }} as created_at  -- When engagement was created
    from
        {{ make_source('hubspot', 'engagements') }}

),

unique_engagements as (
    -- CTE: Deduplicated Engagements
    -- Purpose: Remove duplicate records for the same engagement_id, keeping only
    --          the most recently updated version. This is critical before creating
    --          junction tables to avoid duplicate relationships.
    -- Method: Uses dbt_utils.deduplicate macro with ROW_NUMBER() window function
    -- Output: One row per unique engagement_id (most recent version)

    {{ dbt_utils.deduplicate(
        relation='engagements',  -- Source CTE to deduplicate
        partition_by='engagement_id',  -- Group duplicates by engagement_id
        order_by='updated_at desc',  -- Keep record with latest updated_at
        )
    }}

)

-- Final output: Clean, deduplicated engagement data with association arrays
-- This base model is used by:
--   - stg_hubspot__engagements: For engagement details
--   - stg_hubspot__engagement_contacts: For contact junction table
--   - stg_hubspot__engagement_companies: For company junction table
select * from unique_engagements
