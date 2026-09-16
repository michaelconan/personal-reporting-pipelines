-- ============================================================================
-- CORE LAYER: Sleep Sessions
-- ============================================================================
-- Purpose: Source-agnostic sleep session entity. One row per sleep session
--          regardless of which wearable/API recorded it, so downstream
--          models never depend on a specific provider.
--
-- Source: stg_google_health (currently Google Health; future providers map here)
-- Output: One row per sleep session
-- ============================================================================

select
    sleep_id as session_id,
    sleep_date as session_date,
    started_at,
    ended_at,
    sleep_minutes as duration_minutes,
    asleep_minutes,
    sleep_type,
    is_main_sleep,
    is_nap,
    'google_health' as provider
from
    {{ ref('stg_google_health__sleep') }}
