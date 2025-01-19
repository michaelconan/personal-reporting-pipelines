-- =======================================================================
-- Name: Notion All Habits
-- Description: Merge updated habit data into cleansed table
-- Author: Michael Conan
-- =======================================================================

WITH updated_habits AS (
    SELECT 
        'daily' AS period,
        page_id,
        database_id,
        date,
        habit,
        is_complete,
        page_created,
        page_edited
    FROM
        {{ BQ_DAILY_TABLE }}
    WHERE
        page_edited BETWEEN '{{ data_interval_start | ts }}' AND '{{ data_interval_end | ts }}'
    UNION ALL
    SELECT 
        'weekly' AS period,
        page_id,
        database_id,
        date,
        habit,
        is_complete,
        page_created,
        page_edited
    FROM
        {{ BQ_WEEKLY_TABLE }}
    WHERE
        page_edited BETWEEN '{{ data_interval_start | ts }}' AND '{{ data_interval_end | ts }}'
)

MERGE INTO {{ BQ_HABIT_TABLE }} AS tgt
USING updated_habits AS src
ON tgt.page_id = src.page_id
    AND tgt.date = src.date
    AND tgt.habit = src.habit
WHEN MATCHED THEN
    SET
        is_complete = src.is_complete,
        page_edited = src.page_edited
WHEN NOT MATCHED BY TARGET THEN
    INSERT (period, page_id, database_id, date, habit, is_complete, page_created, page_edited)
    VALUES (period, page_id, database_id, date, habit, is_complete, page_created, page_edited);