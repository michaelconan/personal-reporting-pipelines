-- Singular test: fails if any sleep logs have overlapping time periods.
-- Uses a plain self-join (not a correlated NOT EXISTS) to avoid a known
-- DuckDB assertion failure (duckdb/duckdb#XXXX) with that query pattern.
select
    s1.log_id as log_id_1,
    s2.log_id as log_id_2
from {{ ref('stg_fitbit__sleep') }} s1
inner join {{ ref('stg_fitbit__sleep') }} s2
    on s1.log_id < s2.log_id
where s1.started_at < s2.ended_at
  and s1.ended_at > s2.started_at
