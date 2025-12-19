-- ============================================================================
-- STAGING LAYER: HubSpot Engagement-Contact Junction Table
-- ============================================================================
-- Purpose: Create many-to-many relationship table between engagements and contacts.
--          Unnests JSON array of contact IDs from engagements to create one row
--          per engagement-contact combination.
--
-- Source: base_hubspot__engagements (contains contact_ids JSON array)
-- Transformations:
--   - Unnest contact_ids JSON array (adapter-aware: BigQuery vs DuckDB syntax)
--   - Generate surrogate key for engagement-contact combination
--   - Filter out engagements with no associated contacts
-- Output: One row per engagement-contact relationship
-- Grain: engagement_id + contact_id
-- ============================================================================

select
    -- Generate surrogate key for engagement-contact combination
    -- This ensures uniqueness at the grain of this junction table
    {{ dbt_utils.generate_surrogate_key(['engagement_id', 'contact_id']) }} as row_id,
    engagement_id,  -- HubSpot engagement ID
    contact_id  -- HubSpot contact ID (extracted from JSON array)
from
    {{ ref('base_hubspot__engagements') }},
    -- Unnest JSON array of contact IDs into individual rows
    -- Adapter-aware: Different syntax for BigQuery vs DuckDB
    {% if target.type == 'duckdb' %}
        -- DuckDB: Cast JSON array to varchar array, then unnest
        unnest(cast(json_extract(contact_ids, '$') as varchar[])) as t(contact_id)
    {% elif target.type == 'bigquery' %}
        -- BigQuery: Use json_value_array function to extract array, then unnest
        unnest(json_value_array(contact_ids)) as contact_id
    {% endif %}
where
    -- Filter out engagements with no associated contacts
    contact_ids is not null
