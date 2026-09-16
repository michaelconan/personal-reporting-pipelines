with meetings_contacts as (

    select

        {{ dbt_utils.generate_surrogate_key([

            '_hubspot__meetings_id',

            'to_object_id'

        ]) }} as row_id,

        _hubspot__meetings_id as engagement_id,

        to_object_id as contact_id

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

        to_object_id as contact_id

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

        to_object_id as contact_id

    from {{ source('hubspot', 'communications_to_contacts') }}

    where _hubspot__communications_id is not null

)


select * from meetings_contacts

union all

select * from calls_contacts

union all

select * from communications_contacts
