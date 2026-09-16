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


with stg_hubspot__engagements as (

    select

        engagement_id,

        engagement_type,

        body_preview,

        occurred_at,

        is_synchronous,

        updated_at

    from {{ ref('stg_hubspot__engagements') }}

),


stg_hubspot__engagement_contacts as (

    select

        engagement_id,

        contact_id

    from {{ ref('stg_hubspot__engagement_contacts') }}

),


stg_hubspot__contacts as (

    select

        contact_id,

        first_name,

        last_name,

        company_id

    from {{ ref('stg_hubspot__contacts') }}

),


stg_hubspot__companies as (

    select

        company_id,

        company_name

    from {{ ref('stg_hubspot__companies') }}

),


joined as (

    select

        e.engagement_id,

        e.engagement_type,

        e.body_preview,

        e.occurred_at,

        e.is_synchronous,

        c.contact_id,

        c.first_name,

        c.last_name,

        a.company_id,

        a.company_name,

        e.updated_at

    from

        stg_hubspot__engagements as e

        -- Join to junction table to get engagement-contact relationships

        -- Left join allows engagements without contacts to be included

    left join

        stg_hubspot__engagement_contacts as ec

        on e.engagement_id = ec.engagement_id

        -- Join to contacts to get contact details

        -- Left join allows engagements with contacts not in our contact table

    left join

        stg_hubspot__contacts as c

        on ec.contact_id = c.contact_id

        -- Join to companies to get company details via contact

        -- Left join allows contacts without associated companies

    left join

        stg_hubspot__companies as a

        on c.company_id = a.company_id

)


select

    -- Generate composite surrogate key for engagement-contact combination

    -- This ensures uniqueness at the grain of this table

    {{ dbt_utils.generate_surrogate_key(['engagement_id', 'contact_id']) }} as engagement_key,

    cast(engagement_id as string) as engagement_id,

    engagement_type,

    body_preview,

    occurred_at,

    is_synchronous,

    contact_id,

    first_name,

    last_name,

    company_id,

    company_name,

    updated_at

from

    joined
