-- ============================================================================
-- STAGING LAYER: HubSpot Engagement-Company Junction Table
-- ============================================================================
-- Note: Company associations are not extracted in the current HubSpot pipeline
--       configuration. This model returns an empty result set with the correct
--       schema for downstream compatibility.
-- ============================================================================

select
    cast(null as varchar) as row_id,
    cast(null as integer) as engagement_id,
    cast(null as integer) as company_id
where false
