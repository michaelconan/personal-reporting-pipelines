-- ============================================================================
-- STAGING LAYER: HubSpot Engagement-Company Junction Table
-- ============================================================================
-- Purpose: Create many-to-many relationship table between engagements and companies.
--          Unnests JSON array of company IDs from engagements to create one row
--          per engagement-company combination.
--
-- Source: base_hubspot__engagements (contains company_ids JSON array)
-- Transformations:
--   - Unnest company_ids JSON array (adapter-aware: BigQuery vs DuckDB syntax)
--   - Generate surrogate key for engagement-company combination
--   - Filter out engagements with no associated companies
-- Output: One row per engagement-company relationship
-- Grain: engagement_id + company_id
-- ============================================================================

select
    -- Generate surrogate key for engagement-company combination
    -- This ensures uniqueness at the grain of this junction table
    {{ dbt_utils.generate_surrogate_key(['engagement_id', 'company_id']) }} as row_id,
    engagement_id,  -- HubSpot engagement ID
    company_id  -- HubSpot company ID (extracted from JSON array)
from
    {{ ref('base_hubspot__engagements') }},
    -- Unnest JSON array of company IDs into individual rows
    -- Adapter-aware: Different syntax for BigQuery vs DuckDB
    {% if target.type == 'duckdb' %}
        -- DuckDB: Cast JSON array to varchar array, then unnest
        unnest(cast(json_extract(company_ids, '$') as varchar[])) as t(company_id)
    {% elif target.type == 'bigquery' %}
        -- BigQuery: Use json_value_array function to extract array, then unnest
        unnest(json_value_array(company_ids)) as company_id
    {% endif %}
where
    -- Filter out engagements with no associated companies
    company_ids is not null
