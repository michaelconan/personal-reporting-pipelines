-- ============================================================================
-- MART LAYER: Group Tier Dimension
-- ============================================================================
-- Purpose: Target connection cadence per group tier, feeding relationship
--          adherence reporting. SCD type 1. Includes a sentinel row for groups
--          without a tier.
--
-- Source: group_connect_cadence (seed)
-- Output: One row per group tier (plus an UNKNOWN sentinel row)
-- ============================================================================

with
-- Import CTEs: one per upstream model, selected as-is
group_connect_cadence as (

    select
        group_tier,
        cadence_value,
        cadence_period
    from {{ ref('group_connect_cadence') }}

),

-- Transform CTEs: build the UNKNOWN sentinel and the final output
sentinel as (

    select
        'UNKNOWN_TIER' as group_tier_key,
        cast(null as bigint) as group_tier,
        cast(null as bigint) as cadence_value,
        cast(null as string) as cadence_period

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['group_tier']) }} as group_tier_key,
        cast(group_tier as bigint) as group_tier,
        cast(cadence_value as bigint) as cadence_value,
        cadence_period
    from group_connect_cadence
    union all
    select * from sentinel

)

select * from final
