{{ config(
    materialized='table',
    description='One row per tracked visitor (anonymous_id).',
    indexes=['visitor_key', 'anonymous_id', 'user_id']
) }}

-- Grain: one row per anonymous_id.
--
-- anonymous_id is the tracking cookie, so it is a device+browser, not a
-- person. user_id is carried alongside for the minority of visitors who sign
-- in; joining people across devices would be a separate identity-resolution
-- model and is deliberately not attempted here.

select
    md5(anonymous_id)::uuid                                as visitor_key,
    anonymous_id,

    -- Most recent non-null sign-in seen on this device.
    (array_agg(user_id order by occurred_at desc)
        filter (where user_id is not null))[1]             as user_id,
    bool_or(user_id is not null)                           as is_identified,

    min(occurred_at)                                       as first_seen_at,
    max(occurred_at)                                       as last_seen_at,
    count(*)                                               as lifetime_events,
    count(*) filter (where event_name = 'page_view')       as lifetime_page_views,
    count(distinct order_id)                               as lifetime_orders,
    coalesce(sum(order_revenue_usd), 0)::numeric(12,2)     as lifetime_revenue_usd,

    -- First-touch attributes: what the visitor arrived as, not what they
    -- most recently looked like. Marketing attribution reads these.
    (array_agg(device_type order by occurred_at))[1]       as first_device_type,
    (array_agg(browser order by occurred_at))[1]           as first_browser,
    (array_agg(os order by occurred_at))[1]                as first_os,
    (array_agg(country order by occurred_at))[1]           as first_country,
    (array_agg(utm_source order by occurred_at))[1]        as first_utm_source,
    (array_agg(utm_medium order by occurred_at))[1]        as first_utm_medium,
    (array_agg(utm_campaign order by occurred_at))[1]      as first_utm_campaign

from {{ ref('stg_events') }}
group by anonymous_id
