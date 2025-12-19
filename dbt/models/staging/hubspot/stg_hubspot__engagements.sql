-- ============================================================================
-- STAGING LAYER: HubSpot Engagements
-- ============================================================================
-- Purpose: Clean and standardize HubSpot engagement data. Adds synchronous
--          flag based on engagement type and provides engagement details.
--
-- Source: base_hubspot__engagements (base model with deduplicated engagements)
-- Transformations:
--   - Add is_synchronous flag: true for CALL and MEETING types
--   - Select engagement detail fields
-- Output: One row per unique engagement with synchronous classification
-- ============================================================================

with engagements as (
    -- CTE: Engagements with Synchronous Classification
    -- Purpose: Add business logic to classify engagements as synchronous or
    --          asynchronous. Synchronous engagements are real-time interactions
    --          (calls and meetings), while asynchronous are one-way (emails, notes).
    -- Source: base_hubspot__engagements (deduplicated base model)
    -- Logic: is_synchronous = true if engagement_type is 'MEETING' or 'CALL'

    select
        engagement_id,  -- HubSpot engagement ID
        engagement_type,  -- Type of engagement (EMAIL, CALL, MEETING, NOTE, etc.)
        body_preview,  -- Preview text of engagement content
        occurred_at,  -- Timestamp when engagement occurred
        created_at,  -- Timestamp when engagement was created
        updated_at,  -- Timestamp when engagement was last updated
        -- Classify engagement as synchronous (real-time) or asynchronous (one-way)
        -- Synchronous: MEETING, CALL (require both parties to be present)
        -- Asynchronous: EMAIL, NOTE, TASK, etc. (one-way communication)
        engagement_type in ('MEETING', 'CALL') as is_synchronous
    from
        {{ ref('base_hubspot__engagements') }}

)

-- Final output: Engagement data with synchronous classification
select *
from engagements
