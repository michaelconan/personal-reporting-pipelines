select
    database_id,
    id,
    Date as `date`,
    Name as `name`,
    habit,
    is_complete,
    created_time,
    last_edited_time
from {{ source('notion', 'monthly_habit') }}
    unpivot(
        is_complete for habit in (Budget, Serve, Travel, Blog)
    )
