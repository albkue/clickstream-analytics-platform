{{ config(
    materialized='table',
    description='Daily session/traffic rollup by channel and device.',
    indexes=['session_date', 'channel', 'session_date, channel']
) }}

-- Grain: one row per (session_date, channel, device_type).
--
-- Kept at the session grain rather than the event grain: sessions are the
-- unit almost every traffic question is actually about, and summing
-- page_views up from here still gives the right total.

select
    session_date,
    channel,
    device_type,

    count(*)                                            as sessions,
    count(distinct anonymous_id)                        as visitors,
    sum(events)                                         as events,
    sum(page_views)                                     as page_views,
    sum(searches)                                       as searches,

    count(*) filter (where is_bounce)                   as bounced_sessions,
    count(*) filter (where viewed_product)              as sessions_viewed_product,
    count(*) filter (where added_to_cart)               as sessions_added_to_cart,
    count(*) filter (where started_checkout)            as sessions_started_checkout,
    count(*) filter (where purchased)                   as sessions_purchased,

    sum(orders)                                         as orders,
    sum(revenue_usd)::numeric(12,2)                     as revenue_usd,

    -- Rates are stored, not left to the BI layer, so every consumer computes
    -- them the same way. Denominators use nullif to make an empty group null
    -- rather than a divide-by-zero error.
    round(100.0 * count(*) filter (where is_bounce)
          / nullif(count(*), 0), 2)                     as bounce_rate_pct,
    round(100.0 * count(*) filter (where purchased)
          / nullif(count(*), 0), 2)                     as conversion_rate_pct,
    round(avg(duration_seconds)::numeric, 1)            as avg_session_seconds,
    round(avg(page_views)::numeric, 2)                  as avg_page_views,
    round(sum(revenue_usd) / nullif(count(*), 0), 2)    as revenue_per_session_usd,

    -- Average order value: revenue over orders, not over sessions. Dividing
    -- by sessions here would silently redefine AOV as revenue-per-session.
    round(sum(revenue_usd) / nullif(sum(orders), 0), 2) as avg_order_value_usd

from {{ ref('fct_sessions') }}
group by session_date, channel, device_type
