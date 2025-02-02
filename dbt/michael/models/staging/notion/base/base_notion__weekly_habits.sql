select
    database_id,
    id,
    Name as name,
    habit,
    is_complete,
    created_time,
    last_edited_time
from {{ source('notion', 'weekly_habit') }}
    unpivot(
        is_complete for habit in (Fast, Church)
    )
