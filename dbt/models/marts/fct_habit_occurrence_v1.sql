-- ============================================================================
-- MART LAYER: Habit Occurrence Fact
-- ============================================================================
-- Purpose: One row per habit occurrence per tracking period, with completion
--          resolved against dim_habit.
--
-- Sources: int_habits (occurrences), dim_habit (goal rules), dim_date (keys)
-- Output: One row per habit occurrence
-- ============================================================================

with
-- Import CTEs: one per upstream model, selected as-is
int_habits as (

    select
        habit_key,
        source_id,
        habit_date,
        habit_period,
        habit,
        habit_value
    from {{ ref('int_habits') }}

),

dim_habit as (

    select
        habit_key,
        habit_type,
        threshold,
        is_below_threshold
    from {{ ref('dim_habit', v=1) }}

),

dim_date as (

    select
        date_key,
        date_day
    from {{ ref('dim_date', v=1) }}

),

-- Transform CTE: resolve foreign keys and completion against the goal rules
final as (

    select
        h.habit_key as habit_occurrence_key,
        coalesce(d.date_key, '1900-01-01') as date_key,
        coalesce(g.habit_key, 'UNKNOWN_HABIT') as habit_key,
        h.habit_period,
        h.source_id,
        cast(h.habit_value as numeric(38, 9)) as habit_value,
        case
            when g.habit_type = 'tickbox'
                then h.habit_value = 1.0
            when g.habit_type = 'number' and not coalesce(g.is_below_threshold, false)
                then h.habit_value >= g.threshold
            when g.habit_type = 'number' and coalesce(g.is_below_threshold, false)
                then h.habit_value <= g.threshold
        end as is_complete
    from int_habits as h
    left join dim_habit as g on h.habit = g.habit_key
    left join dim_date as d on h.habit_date = d.date_day

)

select * from final
