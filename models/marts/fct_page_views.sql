{{ config(
    materialized='table',
    description='One row per page_view event, with dwell time and entry/exit flags.',
    indexes=[
        'page_view_id',
        'session_id',
        'occurred_at',
        'page_path, occurred_at',
        'view_date'
    ]
) }}

-- Grain: one row per page_view event.
--
-- The lowest-grain fact in the warehouse. Page-level reporting reads this;
-- session-level reporting reads fct_sessions instead of re-aggregating here.

with page_views as (

    select
        e.event_id                as page_view_id,
        se.session_id,
        se.event_seq_in_session,
        e.anonymous_id,
        e.user_id,
        e.occurred_at,
        e.page_path,
        e.page_title,
        e.page_type,
        e.referrer,
        e.device_type,
        e.browser,
        e.os,
        e.country
    from {{ ref('stg_events') }} e
    join {{ ref('int_sessionized_events') }} se using (event_id)
    where e.event_name = 'page_view'
      and e.page_path is not null

),

sequenced as (

    select
        *,
        row_number() over w                       as page_view_seq,
        lead(occurred_at) over w                  as next_page_view_at,
        count(*) over (partition by session_id)   as session_page_views
    from page_views
    window w as (
        partition by session_id
        order by occurred_at, event_seq_in_session, page_view_id
    )

)

select
    page_view_id,
    session_id,
    anonymous_id,
    md5(anonymous_id)::uuid                          as visitor_key,
    md5(page_path)::uuid                             as page_key,
    user_id,
    occurred_at,
    (occurred_at at time zone 'utc')::date           as view_date,
    page_view_seq,

    page_path,
    page_title,
    page_type,
    referrer,
    device_type,
    browser,
    os,
    country,

    page_view_seq = 1                                as is_entry_page,
    page_view_seq = session_page_views               as is_exit_page,

    -- Time on page is the gap to the next page view in the same session.
    -- The exit page has no next view, so its dwell time is unknowable and
    -- left null rather than imputed as zero -- averaging in a fake zero
    -- would drag every "avg time on page" figure down.
    case
        when next_page_view_at is null then null
        else extract(epoch from (next_page_view_at - occurred_at))::integer
    end                                              as seconds_on_page

from sequenced
