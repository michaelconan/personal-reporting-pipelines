-- ============================================================================
-- MART LAYER: Date Dimension
-- ============================================================================
-- Purpose: Conformed date dimension shared by every datamart fact. One row per
--          calendar day, plus a sentinel row so fact foreign keys never dangle.
--
-- Source: time_spine_daily (daily calendar spine, 2016-01-01 to 2030-12-31)
-- Output: One row per calendar day
-- ============================================================================

with
-- Import CTEs: one per upstream model, selected as-is
time_spine_daily as (

    select
        cast(date_day as date) as date_day
    from {{ ref('time_spine_daily') }}

),

-- Transform CTEs: derive calendar attributes from the spine
calendar_attributes as (

    select
        cast(date_day as string) as date_key,
        date_day,
        cast({{ trunc_date('year', 'date_day') }} as date) as year_start_date,
        cast({{ trunc_date('quarter', 'date_day') }} as date) as quarter_start_date,
        cast({{ trunc_date('month', 'date_day') }} as date) as month_start_date,
        cast({{ trunc_date('week', 'date_day') }} as date) as week_start_date,
        {{ dbt_date.month_name('date_day', False) }} as month_name,
        {{ dbt_date.day_name('date_day', False) }} as day_of_week,
        case
            when {{ dbt_date.day_name('date_day', False) }} in ('Saturday', 'Sunday')
                then true
            else false
        end as is_weekend
    from time_spine_daily

),

sentinel as (

    select
        '1900-01-01' as date_key,
        cast('1900-01-01' as date) as date_day,
        cast('1900-01-01' as date) as year_start_date,
        cast('1900-01-01' as date) as quarter_start_date,
        cast('1900-01-01' as date) as month_start_date,
        cast('1900-01-01' as date) as week_start_date,
        'Unknown' as month_name,
        'Unknown' as day_of_week,
        false as is_weekend

),

final as (

    select * from calendar_attributes
    union all
    select * from sentinel

)

select * from final
