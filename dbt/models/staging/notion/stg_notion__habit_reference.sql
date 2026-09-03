-- ============================================================================
-- STAGING LAYER: Notion Habit Reference
-- ============================================================================
-- Purpose: Clean and standardize the Notion habit reference table, which
--          defines all tracked disciplines with their targets and thresholds.
--          Extracts JSON select/formula fields and derives a canonical habit_key
--          for joining with the habits mart.
--
-- Source: notion.data_source_habit_reference
-- Output: One row per discipline with metadata, targets, and habit_key
-- ============================================================================

with habit_reference as (

    select
        id as page_id,
        {{ extract_notion_title('properties__name__title') }} as habit_name,
        -- Extract select field values from JSON {"name": "..."}
        {{ json_extract_value('properties__category__select', "'$.name'") }} as category,
        lower({{ json_extract_value('properties__frequency__select', "'$.name'") }}) as frequency,
        lower({{ json_extract_value('properties__type__select', "'$.name'") }}) as habit_type,
        {{ json_extract_value('properties__source__select', "'$.name'") }} as source,
        -- Target is stored as a percentage integer (e.g. 80 = 80%)
        properties__target__number as target_pct,
        properties__threshold__number as threshold,
        properties__below_thresholdx__checkbox as below_threshold,
        -- Active is a formula returning JSON {"checkbox": true/false} or {"boolean": true/false}
        coalesce(
            {{ json_extract_value('properties__active__formula', "'$.checkbox'") }},
            {{ json_extract_value('properties__active__formula', "'$.boolean'") }}
        ) = 'true' as active,
        cast(
            left({{ json_extract_value('properties__start_date__date', "'$.start'") }}, 10)
            as date
        ) as start_date,
        created_time as created_at,
        last_edited_time as updated_at
    from {{ make_source('notion', 'data_source_habit_reference') }}

),

with_habit_key as (

    select
        page_id,
        habit_name,
        -- Derive canonical habit_key used in habits_v1:
        --   Tickbox habits → did_{snake_case_name}
        --   HubSpot meeting habits → met_{kind}
        --   Number habits (Notion/Fitbit) → snake_case_name
        category,
        frequency,
        habit_type,
        source,
        target_pct,
        threshold,
        below_threshold,
        active,
        start_date,
        created_at,
        updated_at,
        case
            when habit_type = 'tickbox'
                then 'did_' || lower(replace(habit_name, ' ', '_'))
            when habit_name in ('Meet 1to1', 'Meet Group')
                then 'met_' || lower(replace(replace(habit_name, 'Meet ', ''), ' ', '_'))
            else lower(replace(habit_name, ' ', '_'))
        end as habit_key
    from habit_reference

)

select * from with_habit_key
