"""Read-only queries backing the CLI's reporting commands."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import psycopg

# Every mart the pipeline builds, in the order `status` should list them.
MART_TABLES: tuple[str, ...] = (
    "stg.stg_events",
    "stg.int_sessionized_events",
    "mart.dim_visitor",
    "mart.dim_page",
    "mart.dim_product",
    "mart.fct_sessions",
    "mart.fct_page_views",
    "mart.fct_funnel_steps",
    "mart.agg_daily_traffic",
    "mart.agg_funnel_conversion",
)


@dataclass(frozen=True)
class FunnelRow:
    step_index: int
    step_name: str
    sessions_reached: int
    sessions_lost: int
    step_conversion_pct: float | None
    overall_conversion_pct: float | None
    avg_seconds_to_reach: float | None
    revenue_usd: float


@dataclass(frozen=True)
class TrafficRow:
    session_date: date
    sessions: int
    visitors: int
    page_views: int
    bounce_rate_pct: float | None
    conversion_rate_pct: float | None
    orders: int
    revenue_usd: float
    avg_session_seconds: float | None


@dataclass(frozen=True)
class ChannelRow:
    channel: str
    sessions: int
    visitors: int
    conversion_rate_pct: float | None
    bounce_rate_pct: float | None
    revenue_usd: float
    revenue_per_session_usd: float | None


@dataclass(frozen=True)
class BatchRow:
    batch_id: int
    status: str
    started_at: datetime
    messages_read: int
    rows_inserted: int
    rows_duplicate: int
    rows_rejected: int
    error: str | None


def relation_exists(conn: psycopg.Connection, relation: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,)).fetchone()
    return bool(row[0])


def _require(conn: psycopg.Connection, relation: str) -> None:
    if not relation_exists(conn, relation):
        raise RuntimeError(
            f"{relation} does not exist yet. Run: python -m clickstream transform"
        )


def funnel(conn: psycopg.Connection, channel: str = "(all channels)") -> list[FunnelRow]:
    _require(conn, "mart.agg_funnel_conversion")
    rows = conn.execute(
        """
        SELECT step_index, step_name, sessions_reached, sessions_lost,
               step_conversion_pct, overall_conversion_pct,
               avg_seconds_to_reach, revenue_usd
          FROM mart.agg_funnel_conversion
         WHERE channel = %s
         ORDER BY step_index
        """,
        (channel,),
    ).fetchall()
    return [FunnelRow(*row) for row in rows]


def funnel_channels(conn: psycopg.Connection) -> list[str]:
    _require(conn, "mart.agg_funnel_conversion")
    rows = conn.execute(
        "SELECT DISTINCT channel FROM mart.agg_funnel_conversion ORDER BY channel"
    ).fetchall()
    return [row[0] for row in rows]


def traffic(conn: psycopg.Connection, days: int = 14) -> list[TrafficRow]:
    """Daily totals, rolled up across channel and device."""
    _require(conn, "mart.agg_daily_traffic")
    rows = conn.execute(
        """
        SELECT session_date,
               sum(sessions)                                    AS sessions,
               -- Visitors are summed across device rows, so this slightly
               -- over-counts anyone who used two devices in a day. The exact
               -- figure needs fct_sessions; this is the cheap approximation.
               sum(visitors)                                    AS visitors,
               sum(page_views)                                  AS page_views,
               round(100.0 * sum(bounced_sessions)
                     / nullif(sum(sessions), 0), 2)             AS bounce_rate_pct,
               round(100.0 * sum(sessions_purchased)
                     / nullif(sum(sessions), 0), 2)             AS conversion_rate_pct,
               sum(orders)                                      AS orders,
               sum(revenue_usd)                                 AS revenue_usd,
               round(sum(avg_session_seconds * sessions)
                     / nullif(sum(sessions), 0), 1)             AS avg_session_seconds
          FROM mart.agg_daily_traffic
         GROUP BY session_date
         ORDER BY session_date DESC
         LIMIT %s
        """,
        (days,),
    ).fetchall()
    return [TrafficRow(*row) for row in reversed(rows)]


def channels(conn: psycopg.Connection) -> list[ChannelRow]:
    _require(conn, "mart.agg_daily_traffic")
    rows = conn.execute(
        """
        SELECT channel,
               sum(sessions)                                    AS sessions,
               sum(visitors)                                    AS visitors,
               round(100.0 * sum(sessions_purchased)
                     / nullif(sum(sessions), 0), 2)             AS conversion_rate_pct,
               round(100.0 * sum(bounced_sessions)
                     / nullif(sum(sessions), 0), 2)             AS bounce_rate_pct,
               sum(revenue_usd)                                 AS revenue_usd,
               round(sum(revenue_usd) / nullif(sum(sessions), 0), 2)
                                                                AS revenue_per_session
          FROM mart.agg_daily_traffic
         GROUP BY channel
         ORDER BY sum(sessions) DESC
        """
    ).fetchall()
    return [ChannelRow(*row) for row in rows]


def top_pages(conn: psycopg.Connection, limit: int = 15) -> list[dict[str, Any]]:
    _require(conn, "mart.fct_page_views")
    rows = conn.execute(
        """
        SELECT p.page_path,
               d.page_type,
               count(*)                                         AS views,
               count(DISTINCT p.session_id)                     AS sessions,
               count(*) FILTER (WHERE p.is_entry_page)          AS entries,
               count(*) FILTER (WHERE p.is_exit_page)           AS exits,
               round(avg(p.seconds_on_page)::numeric, 1)        AS avg_seconds,
               round(100.0 * count(*) FILTER (WHERE p.is_exit_page)
                     / nullif(count(*), 0), 1)                  AS exit_rate_pct
          FROM mart.fct_page_views p
          JOIN mart.dim_page d USING (page_path)
         GROUP BY p.page_path, d.page_type
         ORDER BY count(*) DESC
         LIMIT %s
        """,
        (limit,),
    ).fetchall()
    columns = (
        "page_path",
        "page_type",
        "views",
        "sessions",
        "entries",
        "exits",
        "avg_seconds",
        "exit_rate_pct",
    )
    return [dict(zip(columns, row)) for row in rows]


def top_products(conn: psycopg.Connection, limit: int = 10) -> list[dict[str, Any]]:
    _require(conn, "mart.dim_product")
    rows = conn.execute(
        """
        SELECT product_id,
               product_name,
               category,
               latest_price_usd,
               lifetime_product_views                           AS views,
               lifetime_add_to_carts                            AS carts,
               round(100.0 * lifetime_add_to_carts
                     / nullif(lifetime_product_views, 0), 1)    AS cart_rate_pct
          FROM mart.dim_product
         ORDER BY lifetime_product_views DESC
         LIMIT %s
        """,
        (limit,),
    ).fetchall()
    columns = (
        "product_id",
        "product_name",
        "category",
        "price",
        "views",
        "carts",
        "cart_rate_pct",
    )
    return [dict(zip(columns, row)) for row in rows]


def recent_batches(conn: psycopg.Connection, limit: int = 5) -> list[BatchRow]:
    rows = conn.execute(
        """
        SELECT batch_id, status, started_at, messages_read, rows_inserted,
               rows_duplicate, rows_rejected, error
          FROM meta.ingest_batches
         ORDER BY batch_id DESC
         LIMIT %s
        """,
        (limit,),
    ).fetchall()
    return [BatchRow(*row) for row in rows]


def ingest_overview(conn: psycopg.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT count(*),
               count(DISTINCT anonymous_id),
               min(occurred_at),
               max(occurred_at),
               max(ingested_at)
          FROM raw.events
        """
    ).fetchone()
    dead = conn.execute("SELECT count(*) FROM raw.events_dead_letter").fetchone()[0]
    return {
        "events": row[0],
        "visitors": row[1],
        "first_event_at": row[2],
        "last_event_at": row[3],
        "last_ingested_at": row[4],
        "dead_letters": dead,
    }


def table_counts(conn: psycopg.Connection) -> list[tuple[str, int | None]]:
    """Row count per built relation; None where the model has not run yet."""
    counts: list[tuple[str, int | None]] = []
    for relation in MART_TABLES:
        if not relation_exists(conn, relation):
            counts.append((relation, None))
            continue
        counts.append(
            (relation, conn.execute(f"SELECT count(*) FROM {relation}").fetchone()[0])
        )
    return counts


def last_transform_run(conn: psycopg.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT run_id,
               min(started_at)                                  AS started_at,
               max(finished_at)                                 AS finished_at,
               count(*)                                         AS models,
               count(*) FILTER (WHERE status = 'success')       AS succeeded,
               count(*) FILTER (WHERE status = 'failed')        AS failed,
               count(*) FILTER (WHERE status = 'skipped')       AS skipped
          FROM meta.model_runs
         GROUP BY run_id
         ORDER BY min(started_at) DESC
         LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    columns = (
        "run_id",
        "started_at",
        "finished_at",
        "models",
        "succeeded",
        "failed",
        "skipped",
    )
    return dict(zip(columns, row))


def last_test_run(conn: psycopg.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT run_id,
               max(executed_at)                                 AS executed_at,
               count(*)                                         AS tests,
               count(*) FILTER (WHERE status = 'pass')          AS passed,
               count(*) FILTER (WHERE status = 'fail')          AS failed,
               count(*) FILTER (WHERE status = 'error')         AS errored
          FROM meta.test_results
         GROUP BY run_id
         ORDER BY max(executed_at) DESC
         LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    columns = ("run_id", "executed_at", "tests", "passed", "failed", "errored")
    return dict(zip(columns, row))


def failing_tests(conn: psycopg.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT model_name, column_name, test_name, status, failing_rows, error
          FROM meta.test_results
         WHERE run_id = %s AND status <> 'pass'
         ORDER BY model_name, test_name
        """,
        (run_id,),
    ).fetchall()
    columns = (
        "model_name",
        "column_name",
        "test_name",
        "status",
        "failing_rows",
        "error",
    )
    return [dict(zip(columns, row)) for row in rows]
