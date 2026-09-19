-- ============================================================================
-- STAGING LAYER: HubSpot Engagement-Company Junction Table
-- ============================================================================
-- Note: Company associations are not extracted in the current HubSpot pipeline
--       configuration. This model returns an empty result set with the correct
--       schema for downstream compatibility.
-- ============================================================================

with empty_schema as (
    select
        cast(null as string) as row_id,
        cast(null as string) as engagement_id,
        cast(null as string) as company_id
    from (select 1 as _dummy)
)
select * from empty_schema where false
