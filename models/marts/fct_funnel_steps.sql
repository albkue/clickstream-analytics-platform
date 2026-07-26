{{ config(
    materialized='table',
    description='One row per session per funnel step it reached, in order.',
    indexes=[
        'session_id',
        'step_index',
        'channel, step_index',
        'session_date, step_index'
    ]
) }}

-- Grain: one row per (session_id, step_index) actually reached.
--
-- This is a SEQUENTIAL funnel, not a set of independent event counts. A
-- session is credited with step N only if it also reached steps 1..N-1, each
-- no later than the next. That distinction matters: counting add_to_cart
-- events directly would credit a session that deep-linked straight into a
-- product page from an email and skipped the steps before it, and the
-- resulting "conversion rate" could exceed 100% at some steps.

with steps as (

    -- Step order comes from config.FUNNEL_STEPS via the runner, so Python
    -- and SQL cannot disagree about what the funnel is.
    select
        step_name::text        as step_name,
        step_index::integer    as step_index
    from unnest(string_to_array({{ var('funnel_steps') }}, ','))
         with ordinality as t(step_name, step_index)

),

first_hit as (

    -- Earliest occurrence of each funnel event within each session. Later
    -- repeats of the same step do not create new funnel rows.
    select
        se.session_id,
        e.event_name,
        min(e.occurred_at) as reached_at
    from {{ ref('stg_events') }} e
    join {{ ref('int_sessionized_events') }} se using (event_id)
    where e.event_name in (select step_name from steps)
    group by se.session_id, e.event_name

),

ranked as (

    select
        h.session_id,
        s.step_index,
        s.step_name,
        h.reached_at,
        row_number() over (
            partition by h.session_id order by s.step_index
        ) as position,
        lag(h.reached_at) over (
            partition by h.session_id order by s.step_index
        ) as prev_reached_at
    from first_hit h
    join steps s on s.step_name = h.event_name

),

breaks as (

    select
        *,
        case
            -- A gap in the sequence: position lags step_index once a step
            -- is missing, so they stop matching from that point on.
            when step_index <> position then 1
            -- Or the step happened before the one it is supposed to follow.
            when prev_reached_at is not null and reached_at < prev_reached_at then 1
            else 0
        end as is_break
    from ranked

),

contiguous as (

    select
        *,
        sum(is_break) over (
            partition by session_id
            order by step_index
            rows between unbounded preceding and current row
        ) as breaks_so_far
    from breaks

)

select
    c.session_id,
    c.step_index,
    c.step_name,
    c.reached_at,
    s.started_at                                   as session_started_at,
    s.session_date,
    s.channel,
    s.device_type,
    s.country,
    s.anonymous_id,
    s.revenue_usd                                  as session_revenue_usd,

    extract(epoch from (c.reached_at - s.started_at))::integer
                                                   as seconds_from_session_start,

    -- The deepest step this session reached: exactly one row per session is
    -- flagged, which is what makes drop-off ("died here") countable.
    c.step_index = max(c.step_index) over (partition by c.session_id)
                                                   as is_final_step

from contiguous c
join {{ ref('fct_sessions') }} s using (session_id)
where c.breaks_so_far = 0
