with life_habits as (
    -- Notion tickbox habits (daily, weekly, monthly) — 1.0 = done, 0.0 = not done

    select
        row_id as habit_key,
        page_id as source_id,
        page_date as habit_date,
        habit_period,
        habit,
        cast(is_complete as double) as habit_value
    from
        {{ ref('int_habits_unpivoted') }}

),

notion_weekly_numbers as (
    -- Notion number habits: raw values pass through; thresholds applied in metrics layer

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', "'prayer_minutes'"]) }} as habit_key,
        page_id as source_id,
        page_date as habit_date,
        'week' as habit_period,
        'prayer_minutes' as habit,
        cast(coalesce(prayer_minutes, 0) as double) as habit_value
    from {{ ref('stg_notion__weekly_habits') }}

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['page_id', "'screen_minutes'"]) }} as habit_key,
        page_id as source_id,
        page_date as habit_date,
        'week' as habit_period,
        'screen_minutes' as habit,
        cast(screen_minutes as double) as habit_value
    from {{ ref('stg_notion__weekly_habits') }}
    where screen_minutes is not null

),

sleep_habits as (
    -- Fitbit sleep: raw sleep_minutes; threshold comparison deferred to metrics layer

    select
        {{ dbt_utils.generate_surrogate_key(['log_id']) }} as habit_key,
        cast(log_id as varchar) as source_id,
        date_of_sleep as habit_date,
        'day' as habit_period,
        'sleep_minutes' as habit,
        sleep_minutes as habit_value
    from
        {{ ref('stg_fitbit__sleep') }}

),

steps_habits as (
    -- Fitbit steps: raw step count; threshold comparison deferred to metrics layer

    select
        {{ dbt_utils.generate_surrogate_key(['date_of_activity']) }} as habit_key,
        cast(date_of_activity as varchar) as source_id,
        date_of_activity as habit_date,
        'day' as habit_period,
        'steps' as habit,
        cast(steps as double) as habit_value
    from
        {{ ref('stg_fitbit__activities') }}

),

community_meetings as (
    -- Count contacts per synchronous engagement to classify as 1to1 or group

    select
        e.engagement_id,
        e.occurred_at,
        count(ec.contact_id) as contact_count
    from
        {{ ref('stg_hubspot__engagements') }} e
    left join
        {{ ref('stg_hubspot__engagement_contacts') }} ec
        on e.engagement_id = ec.engagement_id
    where
        e.is_synchronous
    group by
        e.engagement_id,
        e.occurred_at

),

community_habits as (
    -- One row per synchronous engagement; metrics layer counts occurrences vs threshold

    select
        {{ dbt_utils.generate_surrogate_key(['engagement_id']) }} as habit_key,
        cast(engagement_id as varchar) as source_id,
        {{ trunc_date('week', 'cast(occurred_at as date)') }} as habit_date,
        'week' as habit_period,
        case
            when contact_count = 1 then 'met_1to1'
            else 'met_group'
        end as habit,
        1.0 as habit_value
    from
        community_meetings

)

select * from life_habits
union all
select * from notion_weekly_numbers
union all
select * from sleep_habits
union all
select * from steps_habits
union all
select * from community_habits
