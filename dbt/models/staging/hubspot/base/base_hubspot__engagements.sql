-- base model to remove duplicate engagements before creating junction tables
with engagements as (

    select
        engagement__id as engagement_id,
        engagement__type as engagement_type,
        engagement__body_preview as body_preview,
        associations__company_ids as company_ids,
        associations__contact_ids as contact_ids,
        engagement__last_updated as updated_at,
        {{ timestamp_parse('engagement__timestamp') }} as occurred_at,
        {{ timestamp_parse('engagement__created_at') }} as created_at
    from
        {{ make_source('hubspot', 'engagements') }}

),

unique_engagements as (

    {{ dbt_utils.deduplicate(
        relation='engagements',
        partition_by='engagement_id',
        order_by='updated_at desc',
        )
    }}

)

select * from unique_engagements
