{{ config(
    materialized='table',
    description='One row per product seen in product or cart events.',
    indexes=['product_key', 'product_id', 'category']
) }}

-- Grain: one row per product_id.
--
-- Prices are captured from the event stream, so this is the price as shown
-- to visitors at the time -- not a join back to an operational catalog. That
-- keeps the mart self-contained and reproducible from the topic alone.

select
    md5(product_id)::uuid                                       as product_key,
    product_id,
    mode() within group (order by product_name)                 as product_name,
    mode() within group (order by product_category)             as category,

    -- Latest observed price, plus the range, so a report can tell whether a
    -- product was discounted during the window it is summarising.
    (array_agg(product_price_usd order by occurred_at desc)
        filter (where product_price_usd is not null))[1]        as latest_price_usd,
    min(product_price_usd)                                      as min_price_usd,
    max(product_price_usd)                                      as max_price_usd,

    count(*) filter (where event_name = 'product_view')         as lifetime_product_views,
    count(*) filter (where event_name = 'add_to_cart')           as lifetime_add_to_carts,
    coalesce(sum(quantity) filter (where event_name = 'add_to_cart'), 0)
                                                                as lifetime_units_added,
    min(occurred_at)                                            as first_seen_at,
    max(occurred_at)                                            as last_seen_at

from {{ ref('stg_events') }}
where product_id is not null
group by product_id
