-- ============================================================================
-- STAGING LAYER: HubSpot Companies
-- ============================================================================
-- Purpose: Clean and standardize raw HubSpot company data. Extracts company
--          information, parses tier from ideal customer profile field, and
--          deduplicates records.
--
-- Source: hubspot.companies (raw dlt-loaded data)
-- Transformations:
--   - Rename columns to standard naming convention
--   - Parse company tier from hs_ideal_customer_profile field (extract numeric)
--   - Deduplicate by company_id (keep most recent)
-- Output: One row per unique company with standardized fields
-- ============================================================================

with companies as (
    -- CTE: Companies Raw Extraction
    -- Purpose: Extract and transform raw HubSpot company data into clean, typed columns.
    --          Handles tier parsing from string field and column renaming.
    -- Source: hubspot.companies (raw API data loaded by dlt)
    -- Transformations:
    --   - Rename columns: id -> company_id, properties__name -> company_name
    --   - Parse tier: Extract numeric value from hs_ideal_customer_profile
    --     (format: "tier_1", "tier_2", "tier_3" -> extract last character)

    select
        id as company_id,  -- HubSpot company ID
        properties__name as company_name,  -- Company name
        properties__createdate as created_at,  -- When company was created in HubSpot
        properties__hs_lastmodifieddate as updated_at,  -- When company was last modified
        -- Parse numeric tier value from string field (e.g., "tier_1" -> 1)
        -- Uses right() to get last character, then safely casts to integer
        {{ cast_safe("right(properties__hs_ideal_customer_profile, 1)", "integer") }} as company_tier
    from
        {{ make_source('hubspot', 'companies') }}

),

unique_companies as (
    -- CTE: Deduplicated Companies
    -- Purpose: Remove duplicate records for the same company_id, keeping only
    --          the most recently updated version.
    -- Method: Uses dbt_utils.deduplicate macro with ROW_NUMBER() window function
    -- Output: One row per unique company_id (most recent version)

    {{ dbt_utils.deduplicate(
        relation='companies',
        partition_by='company_id',
        order_by='updated_at desc'
        )
    }}

)

-- Final output: Clean, deduplicated company data
select * from unique_companies
