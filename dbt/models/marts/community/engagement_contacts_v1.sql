-- ============================================================================
-- MART LAYER: Engagement Contacts
-- ============================================================================
-- Purpose: Denormalized mart table joining HubSpot engagements with associated
--          contacts and companies. This model creates a many-to-many relationship
--          table where one engagement can have multiple contacts, and one contact
--          can have multiple engagements.
--
-- Grain: One row per engagement-contact combination
-- Source: Joins stg_hubspot__engagements, stg_hubspot__engagement_contacts,
--         stg_hubspot__contacts, and stg_hubspot__companies
-- ============================================================================

select
    -- Generate composite surrogate key for engagement-contact combination
    -- This ensures uniqueness at the grain of this table
    {{ dbt_utils.generate_surrogate_key(['e.engagement_id', 'c.contact_id']) }} as engagement_key,
    
    -- Engagement attributes
    e.engagement_id,  -- HubSpot engagement ID
    e.engagement_type,  -- Type of engagement (EMAIL, CALL, MEETING, etc.)
    e.body_preview,  -- Preview text of engagement content
    e.occurred_at,  -- Timestamp when engagement occurred
    e.is_synchronous,  -- Boolean: true for CALL/MEETING, false for EMAIL/etc.
    
    -- Contact attributes
    c.contact_id,  -- HubSpot contact ID
    c.first_name,  -- Contact's first name
    c.last_name,  -- Contact's last name
    
    -- Company attributes (via contact)
    a.company_id,  -- HubSpot company ID associated with contact
    a.company_name,  -- Company name
    
    -- Metadata
    e.updated_at  -- Timestamp when engagement was last updated
from
    {{ ref('stg_hubspot__engagements') }} e
    -- Join to junction table to get engagement-contact relationships
    -- Left join allows engagements without contacts to be included
left join
    {{ ref('stg_hubspot__engagement_contacts') }} ec
    on e.engagement_id = ec.engagement_id
    -- Join to contacts to get contact details
    -- Left join allows engagements with contacts not in our contact table
left join
    {{ ref('stg_hubspot__contacts') }} c
    on ec.contact_id = c.contact_id
    -- Join to companies to get company details via contact
    -- Left join allows contacts without associated companies
left join
    {{ ref('stg_hubspot__companies') }} a
    on c.company_id = a.company_id
