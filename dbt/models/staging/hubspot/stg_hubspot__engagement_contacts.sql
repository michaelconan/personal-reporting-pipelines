-- ============================================================================
-- STAGING LAYER: HubSpot Engagement-Contact Junction Table
-- ============================================================================
-- Purpose: Create many-to-many relationship table between engagements and contacts.
--          Uses pre-built association tables from dlt pipeline instead of
--          unnesting JSON arrays.
--
-- Source: base_hubspot__engagement_contacts
-- Output: One row per engagement-contact relationship with surrogate key
-- Grain: engagement_id + contact_id
-- ============================================================================

select
    {{ dbt_utils.generate_surrogate_key(['engagement_id', 'contact_id']) }} as row_id,
    engagement_id,
    contact_id
from
    {{ ref('base_hubspot__engagement_contacts') }}
