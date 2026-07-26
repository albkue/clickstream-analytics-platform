{{ config(
    materialized='table',
    description='Bridge assigning every event to a derived session.',
    indexes=['event_id', 'session_id', 'anonymous_id, occurred_at']
) }}

-- Server-side sessionization by inactivity gap.
--
-- A new session starts on a visitor's first event, or whenever the gap since
-- their previous event exceeds SESSION_TIMEOUT_MINUTES. Deliberately kept as
-- a narrow bridge (event -> session) rather than a widened copy of
-- stg_events: downstream models join back for attributes, and the sessions
-- can be re-cut with a different timeout without rewriting event data.
--
-- Full rebuild every run, by design. Sessionization is not incremental at the
-- tail: an event arriving now can extend a session whose rows were written on
-- a previous run, and only a full pass sees that.

with ordered as (

    select
        event_id,
        anonymous_id,
        occurred_at,
        lag(occurred_at) over (
            partition by anonymous_id
            order by occurred_at, event_id
        ) as prev_occurred_at
    from {{ ref('stg_events') }}

),

boundaries as (

    -- event_id breaks ties so that events sharing a timestamp order
    -- deterministically; without it the session numbering could differ
    -- between runs on identical data.
    select
        event_id,
        anonymous_id,
        occurred_at,
        case
            when prev_occurred_at is null
              or occurred_at - prev_occurred_at
                 > make_interval(mins => {{ var('session_timeout_minutes') }})
            then 1
            else 0
        end as is_session_start
    from ordered

),

numbered as (

    select
        event_id,
        anonymous_id,
        occurred_at,
        is_session_start,
        -- Running total of the boundary flag: increments only at a gap, so
        -- every event between two gaps shares one session number.
        sum(is_session_start) over (
            partition by anonymous_id
            order by occurred_at, event_id
            rows between unbounded preceding and current row
        ) as session_number
    from boundaries

),

with_session_start as (

    select
        *,
        min(occurred_at) over (
            partition by anonymous_id, session_number
        ) as session_started_at,
        row_number() over (
            partition by anonymous_id, session_number
            order by occurred_at, event_id
        ) as event_seq_in_session
    from numbered

)

select
    event_id,
    anonymous_id,
    occurred_at,
    session_started_at,
    event_seq_in_session,
    -- Hashing (visitor, session start) rather than using the running counter
    -- keeps a session's id stable when older sessions are backfilled ahead
    -- of it; a positional counter would renumber everything after them.
    md5(
        anonymous_id || '|' ||
        to_char(session_started_at at time zone 'utc', 'YYYYMMDDHH24MISSMS')
    )::uuid as session_id
from with_session_start
