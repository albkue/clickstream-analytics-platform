{{ config(
    materialized='incremental',
    unique_key='event_id',
    description='One typed row per validated clickstream event.',
    indexes=[
        'anonymous_id, occurred_at',
        'occurred_at',
        'event_name',
        'ingested_at'
    ]
) }}

-- Flattens raw.events' JSONB into typed columns. Staging does renaming,
-- casting and light cleaning only -- no business logic, no joins, no
-- aggregation. Anything that encodes a decision belongs downstream.

with source as (

    select *
    from {{ source('raw', 'events') }}

    {% if is_incremental() %}
    -- Re-read a 15-minute overlap behind the high-water mark rather than
    -- taking strictly-newer rows. ingested_at is set by the consumer's
    -- transaction, and concurrent consumers commit out of order, so a row
    -- with an earlier ingested_at can become visible after a later one. The
    -- overlap re-selects that window and the unique_key merge makes the
    -- re-selection a no-op instead of a duplicate.
    where ingested_at >= (
        select coalesce(max(ingested_at), '-infinity'::timestamptz)
             - interval '15 minutes'
        from {{ this }}
    )
    {% endif %}

)

select
    event_id,
    event_name,
    anonymous_id,
    nullif(payload ->> 'user_id', '')                     as user_id,
    occurred_at,

    -- page context
    nullif(payload -> 'page' ->> 'path', '')              as page_path,
    nullif(payload -> 'page' ->> 'title', '')             as page_title,
    nullif(payload -> 'page' ->> 'type', '')              as page_type,
    nullif(payload -> 'page' ->> 'referrer', '')          as referrer,

    -- device context
    nullif(payload -> 'device' ->> 'type', '')            as device_type,
    nullif(payload -> 'device' ->> 'browser', '')         as browser,
    nullif(payload -> 'device' ->> 'os', '')              as os,
    nullif(payload -> 'geo' ->> 'country', '')            as country,

    -- acquisition
    nullif(payload -> 'utm' ->> 'source', '')             as utm_source,
    nullif(payload -> 'utm' ->> 'medium', '')             as utm_medium,
    nullif(payload -> 'utm' ->> 'campaign', '')           as utm_campaign,

    -- product context (product_view / add_to_cart)
    nullif(payload -> 'product' ->> 'product_id', '')     as product_id,
    nullif(payload -> 'product' ->> 'name', '')           as product_name,
    nullif(payload -> 'product' ->> 'category', '')       as product_category,
    (payload -> 'product' ->> 'price_usd')::numeric(12,2) as product_price_usd,
    (payload -> 'product' ->> 'quantity')::integer        as quantity,

    -- search
    nullif(payload -> 'search' ->> 'query', '')           as search_query,
    (payload -> 'search' ->> 'results_count')::integer    as search_results_count,

    -- cart snapshot at checkout_start
    (payload -> 'cart' ->> 'value_usd')::numeric(12,2)    as cart_value_usd,
    (payload -> 'cart' ->> 'item_count')::integer         as cart_item_count,

    -- order (purchase)
    nullif(payload -> 'order' ->> 'order_id', '')         as order_id,
    (payload -> 'order' ->> 'revenue_usd')::numeric(12,2) as order_revenue_usd,
    (payload -> 'order' ->> 'shipping_usd')::numeric(12,2) as order_shipping_usd,
    (payload -> 'order' ->> 'item_count')::integer        as order_item_count,

    ingested_at

from source
