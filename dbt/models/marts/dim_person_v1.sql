-- ============================================================================
-- MART LAYER: Person Dimension (relationship master)
-- ============================================================================
-- Purpose: One row per HubSpot contact with the group they belong to. SCD type
--          1 (current snapshot). Includes a sentinel row so engagements with no
--          contact still resolve to a person.
--
-- Source: stg_hubspot__contacts
-- Output: One row per contact (plus an UNKNOWN sentinel row)
-- ============================================================================

with
-- Import CTEs: one per upstream model, selected as-is
stg_hubspot__contacts as (

    select
        contact_id,
        company_id,
        email,
        first_name,
        last_name
    from {{ ref('stg_hubspot__contacts') }}

),

-- Transform CTEs: build the UNKNOWN sentinel and the final output
sentinel as (

    select
        'UNKNOWN_PERSON' as person_key,
        'UNKNOWN_GROUP' as group_key,
        cast(null as string) as contact_id,
        cast(null as string) as email,
        cast(null as string) as first_name,
        cast(null as string) as last_name

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['contact_id']) }} as person_key,
        case
            when company_id is not null
                then {{ dbt_utils.generate_surrogate_key(['company_id']) }}
            else 'UNKNOWN_GROUP'
        end as group_key,
        contact_id,
        email,
        first_name,
        last_name
    from stg_hubspot__contacts
    union all
    select * from sentinel

)

select * from final
