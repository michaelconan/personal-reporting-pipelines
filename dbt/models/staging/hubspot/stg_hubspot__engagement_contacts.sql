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
    {{ unnest_json_array('contact_ids', 'contact_id') }}
where
    -- Filter out engagements with no associated contacts
    contact_ids is not null
