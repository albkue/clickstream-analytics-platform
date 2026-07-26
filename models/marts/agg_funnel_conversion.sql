{{ config(
    materialized='table',
    description='Funnel step-through and drop-off, per channel and overall.',
    indexes=['channel, step_index', 'step_index']
) }}

-- Grain: one row per (channel, step_index), plus an all-channels rollup.
--
-- Two different conversion rates are reported because they answer different
-- questions, and conflating them is the classic funnel reporting bug:
--
--   step_conversion_pct     of the sessions that reached the PREVIOUS step,
--                           how many reached this one? (where the leak is)
--   overall_conversion_pct  of ALL sessions that entered the funnel, how
--                           many got this far? (how big the leak is)

with reached as (

    select
        case
            when grouping(channel) = 1 then '(all channels)'
            else channel
        end                                                   as channel,
        step_index,
        min(step_name)                                        as step_name,
        count(distinct session_id)                            as sessions_reached,
        count(distinct anonymous_id)                          as visitors_reached,
        count(*) filter (where is_final_step)                 as sessions_stopped_here,
        round(avg(seconds_from_session_start)::numeric, 1)    as avg_seconds_to_reach,
        coalesce(sum(session_revenue_usd)
            filter (where step_name = 'purchase'), 0)::numeric(12,2) as revenue_usd
    from {{ ref('fct_funnel_steps') }}
    -- GROUPING SETS gives per-channel rows and the total in one pass, and
    -- guarantees the rollup is consistent with the parts. Summing the
    -- per-channel rows in a dashboard instead would be wrong for the
    -- distinct-visitor columns, since a visitor can appear in two channels.
    group by grouping sets ((channel, step_index), (step_index))

),

with_baselines as (

    select
        *,
        first_value(sessions_reached) over (
            partition by channel order by step_index
        )                                                     as sessions_entered,
        lag(sessions_reached) over (
            partition by channel order by step_index
        )                                                     as prev_step_sessions
    from reached

)

select
    channel,
    step_index,
    step_name,
    sessions_reached,
    visitors_reached,
    sessions_entered,
    prev_step_sessions,

    -- Sessions that reached the previous step and never arrived here.
    coalesce(prev_step_sessions - sessions_reached, 0)         as sessions_lost,
    sessions_stopped_here,

    round(100.0 * sessions_reached
          / nullif(prev_step_sessions, 0), 2)                  as step_conversion_pct,
    round(100.0 * sessions_reached
          / nullif(sessions_entered, 0), 2)                    as overall_conversion_pct,
    round(100.0 * coalesce(prev_step_sessions - sessions_reached, 0)
          / nullif(prev_step_sessions, 0), 2)                  as step_dropoff_pct,

    avg_seconds_to_reach,
    revenue_usd

from with_baselines
