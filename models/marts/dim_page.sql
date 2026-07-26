{{ config(
    materialized='table',
    description='One row per distinct page path seen in the stream.',
    indexes=['page_key', 'page_path', 'page_type']
) }}

-- Grain: one row per page_path.
--
-- Built from observed traffic rather than a static site map, so a page that
-- was never visited does not appear -- which is the right behaviour for a
-- clickstream dimension: it describes what was actually reached.

select
    md5(page_path)::uuid                             as page_key,
    page_path,

    -- Titles and types can drift as a site is edited. The modal value is the
    -- stable label; taking max() or last() would let one stray hit rename
    -- the page for all of history.
    mode() within group (order by page_title)        as page_title,
    mode() within group (order by page_type)         as page_type,

    count(*)                                         as lifetime_page_views,
    count(distinct anonymous_id)                     as lifetime_visitors,
    min(occurred_at)                                 as first_seen_at,
    max(occurred_at)                                 as last_seen_at

from {{ ref('stg_events') }}
where event_name = 'page_view'
  and page_path is not null
group by page_path
