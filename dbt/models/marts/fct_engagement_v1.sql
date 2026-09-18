-- ============================================================================
-- MART LAYER: Engagement Fact
-- ============================================================================
-- Purpose: Relationship-maintenance bridge fact, one row per engagement-contact
--          association, with date, person, and group keys.
--
-- Sources: stg_hubspot__engagements, stg_hubspot__engagement_contacts,
--          stg_hubspot__contacts, dim_date
-- Output: One row per engagement-contact association
-- ============================================================================

with
-- Import CTEs: one per upstream model, selected as-is
stg_hubspot__engagements as (

    select
        engagement_id,
        engagement_type,
        occurred_at,
        is_synchronous
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
        company_id
    from {{ ref('stg_hubspot__contacts') }}

),

dim_date as (

    select
        date_key,
        date_day
    from {{ ref('dim_date', v=1) }}

),

-- Transform CTEs: derive association counts and resolve all foreign keys
engagement_contact_counts as (

    select
        engagement_id,
        cast(count(contact_id) as bigint) as associate_count
    from stg_hubspot__engagement_contacts
    group by engagement_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['e.engagement_id', 'ec.contact_id']) }} as engagement_key,
        coalesce(d.date_key, '1900-01-01') as date_key,
        case
            when ec.contact_id is not null
                then {{ dbt_utils.generate_surrogate_key(['ec.contact_id']) }}
            else 'UNKNOWN_PERSON'
        end as person_key,
        case
            when ct.company_id is not null
                then {{ dbt_utils.generate_surrogate_key(['ct.company_id']) }}
            else 'UNKNOWN_GROUP'
        end as group_key,
        e.engagement_id,
        e.engagement_type,
        e.is_synchronous,
        coalesce(cc.associate_count, 0) as associate_count,
        case
            when cc.associate_count = 1 then '1to1'
            when cc.associate_count >= 2 then 'group'
            else 'unspecified'
        end as engagement_kind
    from stg_hubspot__engagements as e
    left join stg_hubspot__engagement_contacts as ec on e.engagement_id = ec.engagement_id
    left join engagement_contact_counts as cc on e.engagement_id = cc.engagement_id
    left join stg_hubspot__contacts as ct on ec.contact_id = ct.contact_id
    left join dim_date as d on cast(e.occurred_at as date) = d.date_day

)

select * from final
