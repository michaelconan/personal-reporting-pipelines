{{ config(materialized='table') }}

-- Daily time spine table for MetricFlow semantic models
-- Uses dbt.date_spine macro for cross-database compatibility
-- Generates dates from 2020-01-01 to 2030-12-31

{{ dbt.date_spine(
    datepart="day",
    start_date="cast('2020-01-01' as date)",
    end_date="cast('2030-12-31' as date)"
) }}
