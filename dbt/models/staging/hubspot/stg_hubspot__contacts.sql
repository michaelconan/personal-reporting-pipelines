-- ============================================================================
-- STAGING LAYER: HubSpot Contacts
-- ============================================================================
-- Purpose: Clean and standardize raw HubSpot contact data. Extracts contact
--          information, handles company associations, and deduplicates records.
--
-- Source: hubspot.contacts (raw dlt-loaded data)
-- Transformations:
--   - Rename columns to standard naming convention
--   - Extract company association via associatedcompanyid
--   - Deduplicate by contact_id (keep most recent)
-- Output: One row per unique contact with standardized fields
-- ============================================================================

with contacts as (
    -- CTE: Contacts Raw Extraction
    -- Purpose: Extract and transform raw HubSpot contact data into clean, typed columns.
    --          Handles column renaming and company association extraction.
    -- Source: hubspot.contacts (raw API data loaded by dlt)
    -- Transformations:
    --   - Rename columns: id -> contact_id, properties__firstname -> first_name, etc.
    --   - Extract company association: properties__associatedcompanyid -> company_id

    select
        id as contact_id,  -- HubSpot contact ID
        properties__associatedcompanyid as company_id,  -- Associated company ID (can be null)
        properties__email as email,  -- Contact email address
        properties__firstname as first_name,  -- Contact's first name
        properties__lastname as last_name,  -- Contact's last name
        properties__createdate as created_at,  -- When contact was created in HubSpot
        properties__lastmodifieddate as updated_at  -- When contact was last modified
    from
        {{ make_source('hubspot', 'contacts') }}

),

unique_contacts as (
    -- CTE: Deduplicated Contacts
    -- Purpose: Remove duplicate records for the same contact_id, keeping only
    --          the most recently updated version.
    -- Method: Uses dbt_utils.deduplicate macro with ROW_NUMBER() window function
    -- Output: One row per unique contact_id (most recent version)

    {{ dbt_utils.deduplicate(
        relation='contacts',  -- Source CTE to deduplicate
        partition_by='contact_id',  -- Group duplicates by contact_id
        order_by='updated_at desc',  -- Keep record with latest updated_at
        )
    }}

)

-- Final output: Clean, deduplicated contact data
select * from unique_contacts
