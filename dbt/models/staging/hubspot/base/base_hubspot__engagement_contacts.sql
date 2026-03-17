-- ============================================================================
-- STAGING LAYER (BASE): HubSpot Engagement Contacts Base
-- ============================================================================
-- Purpose: Union all engagement-contact association tables into a standardized
--          format. Each row links an engagement to one contact.
--
-- Source: hubspot.meetings_to_contacts, calls_to_contacts, communications_to_contacts
-- Output: One row per engagement-contact relationship
-- ============================================================================

with meetings_contacts as (

    select
        _hubspot__meetings_id as engagement_id,
        to_object_id as contact_id
    from {{ make_source('hubspot', 'meetings_to_contacts') }}
    where _hubspot__meetings_id is not null

),

calls_contacts as (

    select
        _hubspot__calls_id as engagement_id,
        to_object_id as contact_id
    from {{ make_source('hubspot', 'calls_to_contacts') }}
    where _hubspot__calls_id is not null

),

communications_contacts as (

    select
        _hubspot__communications_id as engagement_id,
        to_object_id as contact_id
    from {{ make_source('hubspot', 'communications_to_contacts') }}
    where _hubspot__communications_id is not null

)

select * from meetings_contacts
union all
select * from calls_contacts
union all
select * from communications_contacts
