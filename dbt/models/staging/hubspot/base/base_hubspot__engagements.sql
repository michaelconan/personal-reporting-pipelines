-- ============================================================================
-- STAGING LAYER (BASE): HubSpot Engagements Base
-- ============================================================================
-- Purpose: Union all HubSpot engagement object types (meetings, calls,
--          communications) into a standardized format. Used as the base
--          for stg_hubspot__engagements and junction table models.
--
-- Source: hubspot.meetings, hubspot.calls, hubspot.communications
-- Output: One row per unique engagement across all types
-- ============================================================================

with meetings as (

    select
        id as engagement_id,
        'MEETING' as engagement_type,
        properties__hs_meeting_body as body_preview,
        cast(properties__hs_meeting_start_time as timestamp) as occurred_at,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from {{ make_source('hubspot', 'meetings') }}

),

calls as (

    select
        id as engagement_id,
        'CALL' as engagement_type,
        properties__hs_call_body as body_preview,
        cast(properties__hs_timestamp as timestamp) as occurred_at,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from {{ make_source('hubspot', 'calls') }}

),

communications as (

    select
        id as engagement_id,
        'COMMUNICATION' as engagement_type,
        properties__hs_communication_body as body_preview,
        cast(properties__hs_timestamp as timestamp) as occurred_at,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from {{ make_source('hubspot', 'communications') }}

),

all_engagements as (

    select * from meetings
    union all
    select * from calls
    union all
    select * from communications

),

unique_engagements as (

    {{ dbt_utils.deduplicate(
        relation='all_engagements',
        partition_by='engagement_id',
        order_by='updated_at desc'
        )
    }}

)

select * from unique_engagements
