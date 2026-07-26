"""Command line entry point: python -m clickstream <command>."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

from . import report
from .config import load_catalog, load_settings
from .consumer import consume
from .db import apply_migrations, connect
from .generator import generate_events
from .producer import ensure_topic, publish
from .transform import runner as transform_runner
from .transform import tests as transform_tests

EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_FAILED = 2

log = logging.getLogger("clickstream")


# --------------------------------------------------------------- parser ----


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m clickstream",
        description="Kafka clickstream -> Postgres warehouse -> funnels",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="show debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create/refresh the raw and meta schemas")

    produce = sub.add_parser(
        "produce", help="generate synthetic events and publish them to Kafka"
    )
    produce.add_argument(
        "--sessions", type=int, default=1000, help="sessions to generate (default 1000)"
    )
    produce.add_argument(
        "--days", type=int, default=7, help="spread events over this many days (default 7)"
    )
    produce.add_argument("--seed", type=int, help="seed for a reproducible stream")
    produce.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="events per second; 0 (default) publishes as fast as possible",
    )
    produce.add_argument(
        "--out", metavar="PATH", help="also write the events to a JSON-lines file"
    )
    produce.add_argument(
        "--dry-run",
        action="store_true",
        help="generate and summarise without touching Kafka",
    )

    consume_cmd = sub.add_parser(
        "consume", help="load the topic into raw.events (Ctrl-C to stop)"
    )
    consume_cmd.add_argument(
        "--max-messages", type=int, help="stop after this many messages"
    )
    consume_cmd.add_argument(
        "--idle-timeout",
        type=float,
        help="stop after this many idle seconds (default from .env; 0 = never)",
    )

    transform = sub.add_parser("transform", help="build the dbt-style models")
    transform.add_argument(
        "--select",
        nargs="+",
        metavar="MODEL",
        help="build only these models and their ancestors",
    )
    transform.add_argument(
        "--full-refresh",
        action="store_true",
        help="rebuild incremental models from scratch",
    )

    test_cmd = sub.add_parser("test", help="run the schema tests in models/**/schema.yml")
    test_cmd.add_argument(
        "--select", nargs="+", metavar="MODEL", help="test only these models"
    )

    pipeline = sub.add_parser(
        "pipeline", help="consume, transform and test in one pass"
    )
    pipeline.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help="seconds of stream silence before moving on to transform (default 10)",
    )
    pipeline.add_argument(
        "--full-refresh", action="store_true", help="rebuild incremental models"
    )

    funnel = sub.add_parser("funnel", help="show the conversion funnel")
    funnel.add_argument(
        "--channel",
        default="(all channels)",
        help="channel to report on (default: all)",
    )
    funnel.add_argument(
        "--by-channel", action="store_true", help="show every channel side by side"
    )

    traffic = sub.add_parser("traffic", help="show daily traffic and channel mix")
    traffic.add_argument(
        "--days", type=int, default=14, help="days to show (default 14)"
    )

    pages = sub.add_parser("pages", help="show the most-viewed pages")
    pages.add_argument("--limit", type=int, default=15, help="rows to show (default 15)")

    products = sub.add_parser("products", help="show product engagement")
    products.add_argument(
        "--limit", type=int, default=10, help="rows to show (default 10)"
    )

    status = sub.add_parser("status", help="show pipeline state end to end")
    status.add_argument(
        "--limit", type=int, default=5, help="ingest batches to show (default 5)"
    )

    return parser


# -------------------------------------------------------------- helpers ----


def _fmt(value: object, width: int = 0, dash: str = "-") -> str:
    text = dash if value is None else str(value)
    return text.rjust(width) if width else text


def _bar(fraction: float | None, width: int = 22) -> str:
    if fraction is None:
        return " " * width
    filled = max(0, min(width, round(fraction * width)))
    return "#" * filled + "." * (width - filled)


# ------------------------------------------------------------- commands ----


def cmd_init_db(args: argparse.Namespace) -> int:
    settings = load_settings()
    applied = apply_migrations(settings)
    print(f"Applied {len(applied)} migration file(s): {', '.join(applied)}")
    return EXIT_OK


def cmd_produce(args: argparse.Namespace) -> int:
    settings = load_settings()
    catalog = load_catalog(settings.catalog_file)

    if args.sessions <= 0:
        print("--sessions must be positive", file=sys.stderr)
        return EXIT_FAILED
    if args.days <= 0:
        print("--days must be positive", file=sys.stderr)
        return EXIT_FAILED

    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(days=args.days)
    seed = args.seed if args.seed is not None else settings.generator_seed

    log.info("generating %d sessions over %d day(s)", args.sessions, args.days)
    events = generate_events(
        catalog,
        sessions=args.sessions,
        window_start=window_start,
        window_end=window_end,
        seed=seed,
        session_timeout_minutes=settings.session_timeout_minutes,
    )

    by_name: dict[str, int] = {}
    for event in events:
        by_name[event["event_name"]] = by_name.get(event["event_name"], 0) + 1

    print(f"Generated {len(events)} events from {args.sessions} sessions")
    print(f"  window: {window_start:%Y-%m-%d %H:%M} .. {window_end:%Y-%m-%d %H:%M} UTC")
    for name in sorted(by_name, key=lambda n: -by_name[n]):
        print(f"  {name:<16} {by_name[name]:>7}")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, separators=(",", ":")) + "\n")
        print(f"  wrote {args.out}")

    if args.dry_run:
        print("\n(dry run: nothing published)")
        return EXIT_OK

    created = ensure_topic(settings)
    print(
        f"\nTopic {settings.kafka_topic} "
        f"({'created' if created else 'already exists'})"
    )

    summary = publish(settings, events, rate_per_second=args.rate)
    print(
        f"Published {summary.produced} event(s) in {summary.elapsed_seconds:.1f}s "
        f"({summary.rate:,.0f}/s)"
    )
    if summary.invalid:
        print(f"  {summary.invalid} generated event(s) failed validation")
    if summary.failed:
        print(f"  {summary.failed} delivery failure(s)")
    for error in summary.errors:
        print(f"    {error}")

    return EXIT_OK if summary.ok else EXIT_PARTIAL


def cmd_consume(args: argparse.Namespace) -> int:
    settings = load_settings()
    print(
        f"Consuming {settings.kafka_topic} as group "
        f"{settings.kafka_consumer_group} (Ctrl-C to stop)"
    )
    summary = consume(
        settings,
        max_messages=args.max_messages,
        idle_timeout_seconds=args.idle_timeout,
    )
    print(
        f"\nRead {summary.messages_read} message(s) in {summary.batches} batch(es), "
        f"{summary.elapsed_seconds:.1f}s ({summary.rate:,.0f}/s) "
        f"-- stopped: {summary.stopped_because}"
    )
    print(f"  inserted   {summary.rows_inserted}")
    print(f"  duplicate  {summary.rows_duplicate}  (already in raw.events)")
    print(f"  rejected   {summary.rows_rejected}  (see raw.events_dead_letter)")
    return EXIT_PARTIAL if summary.rows_rejected else EXIT_OK


def cmd_transform(args: argparse.Namespace) -> int:
    settings = load_settings()
    summary = transform_runner.run(
        settings, select=args.select, full_refresh=args.full_refresh
    )

    print(f"\nTransform run {summary.run_id}: {summary.status.upper()}")
    print(f"{'model':<28} {'materialized':<13} {'rows':>9}  {'time':>7}  status")
    for result in summary.results:
        rows = "-" if result.rows is None else f"{result.rows:,}"
        print(
            f"{result.model.name:<28} {result.model.materialized:<13} {rows:>9}  "
            f"{result.duration_seconds:>6.2f}s  {result.status}"
        )
        if result.error:
            print(f"    {result.error}")

    counts = summary.counts()
    print(
        f"\n{counts['success']} succeeded, {counts['failed']} failed, "
        f"{counts['skipped']} skipped in {summary.elapsed_seconds:.1f}s"
    )
    return {
        "success": EXIT_OK,
        "partial": EXIT_PARTIAL,
        "failed": EXIT_FAILED,
    }[summary.status]


def cmd_test(args: argparse.Namespace) -> int:
    settings = load_settings()
    summary = transform_tests.run(settings, select=args.select)

    counts = summary.counts()
    total = sum(counts.values())
    print(f"\nSchema tests: {counts['pass']}/{total} passed")

    for result in summary.results:
        if result.status == "pass":
            continue
        target = f"{result.test.model_name}.{result.test.column_name or '*'}"
        detail = result.error or f"{result.failing_rows} failing row(s)"
        print(f"  {result.status.upper():<5} {target:<34} {result.test.test_name}")
        print(f"        {detail}")

    if counts["fail"] or counts["error"]:
        return EXIT_FAILED
    return EXIT_OK


def cmd_pipeline(args: argparse.Namespace) -> int:
    """Consume whatever is on the topic, then rebuild and test the warehouse."""
    consume_args = argparse.Namespace(
        max_messages=None, idle_timeout=args.idle_timeout
    )
    print("=" * 68)
    print("1/3  ingest")
    print("=" * 68)
    ingest_code = cmd_consume(consume_args)

    print("\n" + "=" * 68)
    print("2/3  transform")
    print("=" * 68)
    transform_code = cmd_transform(
        argparse.Namespace(select=None, full_refresh=args.full_refresh)
    )
    if transform_code == EXIT_FAILED:
        return EXIT_FAILED

    print("\n" + "=" * 68)
    print("3/3  test")
    print("=" * 68)
    test_code = cmd_test(argparse.Namespace(select=None))

    return max(ingest_code, transform_code, test_code)


def cmd_funnel(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        channels = (
            report.funnel_channels(conn) if args.by_channel else [args.channel]
        )
        for channel in channels:
            rows = report.funnel(conn, channel)
            if not rows:
                print(f"\n{channel}: no sessions")
                continue

            entered = rows[0].sessions_reached
            print(f"\nConversion funnel -- {channel}  ({entered:,} sessions entered)")
            print(
                f"  {'step':<16} {'sessions':>9} {'of prev':>8} {'of all':>7} "
                f"{'lost':>7}  {'':<22}"
            )
            for row in rows:
                overall = row.overall_conversion_pct
                step = row.step_conversion_pct
                print(
                    f"  {row.step_name:<16} {row.sessions_reached:>9,} "
                    f"{_fmt(step, 7)}% {_fmt(overall, 6)}% "
                    f"{row.sessions_lost:>7,}  "
                    f"{_bar(float(overall) / 100 if overall is not None else None)}"
                )
            revenue = rows[-1].revenue_usd or 0
            if revenue:
                print(f"  {'revenue':<16} ${revenue:>12,.2f}")
    return EXIT_OK


def cmd_traffic(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        rows = report.traffic(conn, args.days)
        if not rows:
            print("No traffic yet. Run: python -m clickstream transform")
            return EXIT_OK

        print(f"Daily traffic (last {len(rows)} day(s), UTC)")
        print(
            f"  {'date':<12} {'sessions':>9} {'visitors':>9} {'views':>8} "
            f"{'bounce':>7} {'conv':>6} {'orders':>7} {'revenue':>11} {'avg sec':>8}"
        )
        for row in rows:
            print(
                f"  {row.session_date!s:<12} {row.sessions:>9,} {row.visitors:>9,} "
                f"{row.page_views:>8,} {_fmt(row.bounce_rate_pct, 6)}% "
                f"{_fmt(row.conversion_rate_pct, 5)}% {row.orders:>7,} "
                f"${row.revenue_usd:>10,.2f} {_fmt(row.avg_session_seconds, 8)}"
            )

        print("\nBy acquisition channel")
        print(
            f"  {'channel':<16} {'sessions':>9} {'conv':>6} {'bounce':>7} "
            f"{'revenue':>12} {'rev/session':>12}"
        )
        for channel in report.channels(conn):
            print(
                f"  {channel.channel:<16} {channel.sessions:>9,} "
                f"{_fmt(channel.conversion_rate_pct, 5)}% "
                f"{_fmt(channel.bounce_rate_pct, 6)}% "
                f"${channel.revenue_usd:>11,.2f} "
                f"${_fmt(channel.revenue_per_session_usd, 11)}"
            )
    return EXIT_OK


def cmd_pages(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        rows = report.top_pages(conn, args.limit)
        print(f"Top {len(rows)} pages by views")
        print(
            f"  {'path':<26} {'type':<13} {'views':>8} {'sessions':>9} "
            f"{'entries':>8} {'exit %':>7} {'avg sec':>8}"
        )
        for row in rows:
            print(
                f"  {row['page_path']:<26} {row['page_type']:<13} "
                f"{row['views']:>8,} {row['sessions']:>9,} {row['entries']:>8,} "
                f"{_fmt(row['exit_rate_pct'], 6)}% {_fmt(row['avg_seconds'], 8)}"
            )
    return EXIT_OK


def cmd_products(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        rows = report.top_products(conn, args.limit)
        print(f"Top {len(rows)} products by product views")
        print(
            f"  {'product':<24} {'category':<10} {'price':>9} {'views':>7} "
            f"{'carts':>7} {'cart %':>7}"
        )
        for row in rows:
            print(
                f"  {row['product_name']:<24} {row['category']:<10} "
                f"${row['price']:>8,.2f} {row['views']:>7,} {row['carts']:>7,} "
                f"{_fmt(row['cart_rate_pct'], 6)}%"
            )
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    settings = load_settings()
    with connect(settings) as conn:
        overview = report.ingest_overview(conn)
        print("Ingestion (raw.events, times in UTC)")
        print(f"  events            {overview['events']:,}")
        print(f"  distinct visitors {overview['visitors']:,}")
        if overview["first_event_at"]:
            print(
                f"  event window      {overview['first_event_at']:%Y-%m-%d %H:%M} "
                f".. {overview['last_event_at']:%Y-%m-%d %H:%M}"
            )
            print(f"  last ingested     {overview['last_ingested_at']:%Y-%m-%d %H:%M:%S}")
        if overview["dead_letters"]:
            print(f"  dead letters      {overview['dead_letters']:,}  (raw.events_dead_letter)")

        print(f"\nRecent ingest batches (last {args.limit})")
        batches = report.recent_batches(conn, args.limit)
        if not batches:
            print("  (none yet)")
        for batch in batches:
            print(
                f"  #{batch.batch_id:<5} {batch.status:<10} "
                f"{batch.started_at:%Y-%m-%d %H:%M:%S}  "
                f"read {batch.messages_read:>6}  "
                f"inserted {batch.rows_inserted:>6}  "
                f"dup {batch.rows_duplicate:>5}  rej {batch.rows_rejected:>4}"
            )
            if batch.error:
                print(f"        error: {batch.error[:150]}")

        print("\nWarehouse tables")
        for relation, count in report.table_counts(conn):
            value = "not built" if count is None else f"{count:,}"
            print(f"  {relation:<32} {value:>12}")

        last_run = report.last_transform_run(conn)
        print("\nLast transform run")
        if last_run is None:
            print("  (none yet) -- run: python -m clickstream transform")
        else:
            print(
                f"  {last_run['run_id']}  {last_run['started_at']:%Y-%m-%d %H:%M:%S}"
            )
            print(
                f"  {last_run['succeeded']}/{last_run['models']} models succeeded, "
                f"{last_run['failed']} failed, {last_run['skipped']} skipped"
            )

        last_test = report.last_test_run(conn)
        print("\nLast schema test run")
        if last_test is None:
            print("  (none yet) -- run: python -m clickstream test")
        else:
            print(
                f"  {last_test['executed_at']:%Y-%m-%d %H:%M:%S}  "
                f"{last_test['passed']}/{last_test['tests']} passed, "
                f"{last_test['failed']} failed, {last_test['errored']} errored"
            )
            for failure in report.failing_tests(conn, str(last_test["run_id"])):
                target = f"{failure['model_name']}.{failure['column_name'] or '*'}"
                detail = failure["error"] or f"{failure['failing_rows']} row(s)"
                print(f"    {failure['status'].upper():<5} {target:<32} "
                      f"{failure['test_name']}: {detail}")

    return EXIT_OK


# ----------------------------------------------------------------- main ----


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    handlers = {
        "init-db": cmd_init_db,
        "produce": cmd_produce,
        "consume": cmd_consume,
        "transform": cmd_transform,
        "test": cmd_test,
        "pipeline": cmd_pipeline,
        "funnel": cmd_funnel,
        "traffic": cmd_traffic,
        "pages": cmd_pages,
        "products": cmd_products,
        "status": cmd_status,
    }
    try:
        return handlers[args.command](args)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return EXIT_FAILED
    except Exception as exc:  # surface a readable message, not a raw traceback
        log.error("%s: %s", type(exc).__name__, exc)
        if args.verbose:
            raise
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
