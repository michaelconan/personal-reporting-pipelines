-- ============================================================================
-- MART LAYER: Habits
-- ============================================================================
-- Purpose: Final habits reporting table. One row per habit occurrence per
--          tracking period across every source system.
--
-- Source: int_habits (unified habit occurrences)
-- Output: One row per habit occurrence
-- ============================================================================

select
    habit_key,
    source_id,
    habit_date,
    habit_period,
    habit,
    habit_value
from
    {{ ref('int_habits') }}
