-- base model to remove duplicate engagements before creating junction tables
with engagements as (

    select
        engagement__id as engagement_id,
        engagement__type as engagement_type,
        {% if target.type == 'bigquery' %}
            timestamp_millis(engagement__timestamp)
        {% elif target.type == 'duckdb' %}
            make_timestamp(cast(engagement__timestamp as bigint) * 1000000)
        {% endif %}
          as occurred_at,
        engagement__body_preview as body_preview,
        associations__company_ids as company_ids,
        associations__contact_ids as contact_ids,
        {% if target.type == 'bigquery' %}
            timestamp_millis(engagement__created_at)
        {% elif target.type == 'duckdb' %}
            make_timestamp(cast(engagement__created_at as bigint) * 1000000)
        {% endif %}
          as created_at,
        engagement__last_updated as updated_at
    from
        {% if target.name == 'dev' %}
            {{ ref('hubspot__engagements') }}
        {% else %}
            {{ source('hubspot', 'engagements') }}
        {% endif %}

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
