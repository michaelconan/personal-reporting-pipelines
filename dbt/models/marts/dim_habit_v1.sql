-- ============================================================================
-- MART LAYER: Habit Dimension (goal master)
-- ============================================================================
-- Purpose: One row per tracked discipline, carrying the goal targets and
--          thresholds used to evaluate completion on fct_habit_occurrence. SCD
--          type 1 (current snapshot). The canonical habit_key doubles as the
--          surrogate key because it is unique and immutable.
--
-- Source: stg_notion__habit_reference
-- Output: One row per habit (plus an UNKNOWN sentinel row)
-- ============================================================================

with
-- Import CTEs: one per upstream model, selected as-is
stg_notion__habit_reference as (

    select
        habit_key,
        page_id,
        habit_name,
        category,
        frequency,
        habit_type,
        source,
        cast(target_pct as numeric) as target_pct,
        cast(threshold as numeric) as threshold,
        coalesce(is_below_threshold, false) as is_below_threshold,
        coalesce(is_active, false) as is_active,
        cast(start_date as date) as start_date
    from {{ ref('stg_notion__habit_reference') }}

),

-- Transform CTEs: add the UNKNOWN sentinel and shape the output columns
sentinel as (

    select
        'UNKNOWN_HABIT' as habit_key,
        cast(null as string) as page_id,
        'Unknown habit' as habit_name,
        cast(null as string) as category,
        cast(null as string) as frequency,
        cast(null as string) as habit_type,
        cast(null as string) as source,
        cast(null as numeric) as target_pct,
        cast(null as numeric) as threshold,
        false as is_below_threshold,
        false as is_active,
        cast(null as date) as start_date

),

final as (

    select
        habit_key,
        page_id as source_id,
        habit_name,
        category,
        frequency,
        habit_type,
        source,
        target_pct,
        threshold,
        is_below_threshold,
        is_active,
        start_date
    from stg_notion__habit_reference
    union all
    select
        habit_key,
        page_id as source_id,
        habit_name,
        category,
        frequency,
        habit_type,
        source,
        target_pct,
        threshold,
        is_below_threshold,
        is_active,
        start_date
    from sentinel

)

select * from final
