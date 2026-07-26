{{ config(
    materialized='table',
    description='One row per derived session, with funnel depth and revenue.',
    indexes=[
        'session_id',
        'anonymous_id',
        'started_at',
        'channel, started_at',
        'session_date'
    ]
) }}

-- Grain: one row per session_id.
--
-- The central fact of the platform: everything about a visit collapsed into
-- one row, so "how many sessions converted" is a filtered count rather than
-- a re-derivation from events.

with session_events as (

    select
        se.session_id,
        se.session_started_at,
        se.event_seq_in_session,
        e.*
    from {{ ref('stg_events') }} e
    join {{ ref('int_sessionized_events') }} se using (event_id)

),

aggregated as (

    select
        session_id,
        min(anonymous_id)                                        as anonymous_id,
        min(session_started_at)                                  as started_at,
        max(occurred_at)                                         as ended_at,

        count(*)                                                 as events,
        count(*) filter (where event_name = 'page_view')          as page_views,
        count(distinct page_path)
            filter (where event_name = 'page_view')               as unique_pages,
        count(*) filter (where event_name = 'search')             as searches,

        -- Funnel flags. bool_or over the session is enough for "did it
        -- happen"; strict step ordering is fct_funnel_steps' job.
        bool_or(event_name = 'product_view')                      as viewed_product,
        bool_or(event_name = 'add_to_cart')                       as added_to_cart,
        bool_or(event_name = 'checkout_start')                    as started_checkout,
        bool_or(event_name = 'purchase')                          as purchased,

        count(distinct product_id)
            filter (where event_name = 'product_view')            as products_viewed,
        count(distinct order_id)                                  as orders,
        coalesce(sum(order_revenue_usd), 0)::numeric(12,2)        as revenue_usd,

        -- Signed-in at any point during the visit.
        (array_agg(user_id order by occurred_at desc)
            filter (where user_id is not null))[1]                as user_id,

        -- Session-level context, taken from the first event of the visit.
        (array_agg(device_type order by event_seq_in_session))[1]  as device_type,
        (array_agg(browser order by event_seq_in_session))[1]      as browser,
        (array_agg(os order by event_seq_in_session))[1]           as os,
        (array_agg(country order by event_seq_in_session))[1]      as country,
        (array_agg(utm_source order by event_seq_in_session))[1]   as utm_source,
        (array_agg(utm_medium order by event_seq_in_session))[1]   as utm_medium,
        (array_agg(utm_campaign order by event_seq_in_session))[1] as utm_campaign,
        (array_agg(referrer order by event_seq_in_session)
            filter (where referrer is not null))[1]                as landing_referrer,

        (array_agg(page_path order by event_seq_in_session)
            filter (where event_name = 'page_view'))[1]            as entry_page_path,
        (array_agg(page_path order by event_seq_in_session desc)
            filter (where event_name = 'page_view'))[1]            as exit_page_path

    from session_events
    group by session_id

)

select
    session_id,
    anonymous_id,
    md5(anonymous_id)::uuid                       as visitor_key,
    user_id,
    started_at,
    ended_at,
    (started_at at time zone 'utc')::date         as session_date,

    -- Duration is last-event minus first-event. A single-hit session gets 0,
    -- not null: the visit happened, we just cannot see how long it lasted
    -- (there is no unload beacon in this stream).
    extract(epoch from (ended_at - started_at))::integer as duration_seconds,

    events,
    page_views,
    unique_pages,
    searches,

    -- Bounce: one page view and nothing else. Defined on page views rather
    -- than on total events so a session that viewed one page but searched or
    -- added to cart is correctly not a bounce.
    (page_views <= 1 and not viewed_product
        and not added_to_cart and not started_checkout
        and not purchased)                        as is_bounce,

    viewed_product,
    added_to_cart,
    started_checkout,
    purchased,
    products_viewed,
    orders,
    revenue_usd,

    device_type,
    browser,
    os,
    country,
    utm_source,
    utm_medium,
    utm_campaign,
    landing_referrer,
    entry_page_path,
    exit_page_path,

    -- Channel grouping. Defined once, here, and read by every aggregate --
    -- the same CASE copy-pasted into three marts is how two dashboards end
    -- up disagreeing about what "social" means.
    case
        when utm_medium in ('cpc', 'ppc', 'paid_search')  then 'paid_search'
        when utm_medium = 'paid_social'                    then 'paid_social'
        when utm_medium = 'social'                         then 'organic_social'
        when utm_medium = 'email'                          then 'email'
        when utm_medium = 'organic'                        then 'organic_search'
        when utm_medium = 'referral'                       then 'referral'
        when utm_source is null and utm_medium is null     then 'direct'
        else 'other'
    end                                           as channel

from aggregated
