with meetings_contacts as (

    select

        {{ dbt_utils.generate_surrogate_key([

            '_hubspot__meetings_id',

            'to_object_id'

        ]) }} as row_id,

        _hubspot__meetings_id as engagement_id,

        cast(to_object_id as string) as contact_id,

        _hubspot__meetings_updated_at as updated_at,

        _dlt_load_id

    from {{ source('hubspot', 'meetings_to_contacts') }}

    where _hubspot__meetings_id is not null

),


calls_contacts as (

    select

        {{ dbt_utils.generate_surrogate_key([

            '_hubspot__calls_id',

            'to_object_id'

        ]) }} as row_id,

        _hubspot__calls_id as engagement_id,

        cast(to_object_id as string) as contact_id,

        _hubspot__calls_updated_at as updated_at,

        _dlt_load_id

    from {{ source('hubspot', 'calls_to_contacts') }}

    where _hubspot__calls_id is not null

),


communications_contacts as (

    select

        {{ dbt_utils.generate_surrogate_key([

            '_hubspot__communications_id',

            'to_object_id'

        ]) }} as row_id,

        _hubspot__communications_id as engagement_id,

        cast(to_object_id as string) as contact_id,

        _hubspot__communications_updated_at as updated_at,

        _dlt_load_id

    from {{ source('hubspot', 'communications_to_contacts') }}

    where _hubspot__communications_id is not null

),


all_engagement_contacts as (

    select * from meetings_contacts

    union all

    select * from calls_contacts

    union all

    select * from communications_contacts

),

unique_engagement_contacts as (
-- CTE: Deduplicated association rows
-- Purpose: The pipeline appends association rows on every load (include_from_parent
--          parent version), so the same engagement-contact pair can appear many
--          times. Keep the most recently updated version of each pair, like the
--          base engagement model does.
    {{ dbt_utils.deduplicate(
        relation='all_engagement_contacts',
        partition_by='row_id',
        order_by='updated_at desc, _dlt_load_id desc'
        )
    }}

)

select * from unique_engagement_contacts
