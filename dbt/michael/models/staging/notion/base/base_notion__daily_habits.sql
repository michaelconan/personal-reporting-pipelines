select
    database_id,
    id,
    Name as name,
    habit,
    is_complete,
    created_time,
    last_edited_time
from {{ source('notion', 'daily_habit') }}
    unpivot(
        is_complete for habit in (Devotional, Journal, Prayer, `Read Bible`, Workout, Language)
    )
