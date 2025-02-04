-- union daily and weekly habits from base models
select
    database_id,
    'daily' as period,
    id,
    name,
    habit,
    is_complete,
    created_time,
    last_edited_time
from {{ ref('base_notion__daily_habits') }}

union all

select
    database_id,
    'weekly' as period,
    id,
    name,
    habit,
    is_complete,
    created_time,
    last_edited_time
from {{ ref('base_notion__weekly_habits') }}