{{ config(materialized='table', contract={'enforced': True}) }}

-- Daily time spine table for MetricFlow semantic models
-- Uses dbt.date_spine macro for cross-database compatibility
-- Generates dates from 2020-01-01 to 2030-12-31

with spine as (

    {{ dbt.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}

)

-- date_spine emits timestamps; cast to a date for the contract time grain
select cast(date_day as date) as date_day
from
    spine
