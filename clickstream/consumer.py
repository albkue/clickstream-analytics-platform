"""Consume the Kafka topic into raw.events.

Delivery semantics: at-least-once from Kafka, effectively-once in the
warehouse. The ordering is what buys that --

    1. buffer messages,
    2. write rows + the batch record in ONE Postgres transaction,
    3. only then commit Kafka offsets.

A crash between (2) and (3) replays the batch; every replayed row collides
with raw.events' primary key on event_id and is dropped by ON CONFLICT DO
NOTHING. A crash before (2) loses nothing, because the offsets were never
advanced. What is never possible is committing offsets for rows that failed
to land.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from psycopg.types.json import Jsonb

from .config import Settings
from .events import Event, EventValidationError, decode_message

log = logging.getLogger(__name__)

_RAW_COLUMNS = (
    "event_id",
    "event_name",
    "anonymous_id",
    "occurred_at",
    "payload",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "batch_id",
)


@dataclass
class ConsumeSummary:
    batches: int = 0
    messages_read: int = 0
    rows_inserted: int = 0
    rows_duplicate: int = 0
    rows_rejected: int = 0
    elapsed_seconds: float = 0.0
    stopped_because: str = "idle"

    @property
    def rate(self) -> float:
        return self.messages_read / self.elapsed_seconds if self.elapsed_seconds else 0.0


@dataclass
class _Rejected:
    partition: int
    offset: int
    key: str | None
    value: str | None
    error: str


@dataclass
class _Batch:
    """Messages buffered since the last flush."""

    topic: str
    events: list[tuple[Event, int, int]] = field(default_factory=list)
    rejected: list[_Rejected] = field(default_factory=list)
    # partition -> highest offset seen, so we know where to resume.
    max_offsets: dict[int, int] = field(default_factory=dict)
    opened_at: float = field(default_factory=time.monotonic)

    def __len__(self) -> int:
        return len(self.events) + len(self.rejected)

    def note_offset(self, partition: int, offset: int) -> None:
        current = self.max_offsets.get(partition)
        if current is None or offset > current:
            self.max_offsets[partition] = offset

    def commit_offsets(self) -> list[TopicPartition]:
        # Kafka commits the NEXT offset to read, not the last one read.
        return [
            TopicPartition(self.topic, partition, offset + 1)
            for partition, offset in self.max_offsets.items()
        ]


def build_consumer(settings: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "client.id": "clickstream-consumer",
            # Offsets are committed by hand after the database transaction
            # commits. Auto-commit would advance them on a timer, which is
            # exactly the way to lose a batch.
            "enable.auto.commit": False,
            # A brand-new group reads the topic from the beginning so the
            # warehouse can be rebuilt from retained history.
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        }
    )


def _flush(
    conn: psycopg.Connection,
    settings: Settings,
    batch: _Batch,
    summary: ConsumeSummary,
) -> None:
    """Write one batch inside a single transaction."""
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta.ingest_batches
                    (consumer_group, topic, messages_read, offsets)
                VALUES (%s, %s, %s, %s)
                RETURNING batch_id
                """,
                (
                    settings.kafka_consumer_group,
                    batch.topic,
                    len(batch),
                    Jsonb({str(p): o for p, o in batch.max_offsets.items()}),
                ),
            )
            batch_id = cur.fetchone()[0]

            inserted = 0
            if batch.events:
                # Staged through a temp table so the whole batch lands in one
                # COPY, and so intra-batch duplicates (a redelivery inside the
                # same buffer) are collapsed before they reach the insert.
                cur.execute(
                    "CREATE TEMP TABLE _stage_events "
                    "(LIKE raw.events INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                columns = ", ".join(_RAW_COLUMNS)
                with cur.copy(
                    f"COPY _stage_events ({columns}) FROM STDIN"
                ) as copy:
                    for event, partition, offset in batch.events:
                        copy.write_row(
                            (
                                event.event_id,
                                event.event_name,
                                event.anonymous_id,
                                event.occurred_at,
                                Jsonb(event.payload),
                                batch.topic,
                                partition,
                                offset,
                                batch_id,
                            )
                        )

                cur.execute(
                    f"""
                    INSERT INTO raw.events ({columns})
                    SELECT DISTINCT ON (event_id) {columns}
                    FROM _stage_events
                    ORDER BY event_id, kafka_offset
                    ON CONFLICT (event_id) DO NOTHING
                    """
                )
                inserted = cur.rowcount

            if batch.rejected:
                cur.executemany(
                    """
                    INSERT INTO raw.events_dead_letter
                        (batch_id, kafka_topic, kafka_partition, kafka_offset,
                         message_key, message_value, error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            batch_id,
                            batch.topic,
                            item.partition,
                            item.offset,
                            item.key,
                            item.value,
                            item.error,
                        )
                        for item in batch.rejected
                    ],
                )

            duplicates = len(batch.events) - inserted
            cur.execute(
                """
                UPDATE meta.ingest_batches
                   SET status = 'committed',
                       finished_at = now(),
                       rows_inserted = %s,
                       rows_duplicate = %s,
                       rows_rejected = %s
                 WHERE batch_id = %s
                """,
                (inserted, duplicates, len(batch.rejected), batch_id),
            )

    summary.batches += 1
    summary.messages_read += len(batch)
    summary.rows_inserted += inserted
    summary.rows_duplicate += duplicates
    summary.rows_rejected += len(batch.rejected)

    log.info(
        "batch %d: %d inserted, %d duplicate, %d rejected",
        batch_id,
        inserted,
        duplicates,
        len(batch.rejected),
    )


def _for_storage(raw: bytes | None, limit: int = 8000) -> str | None:
    """Make arbitrary message bytes safe to store in a text column.

    A dead letter is by definition a message we could not parse, so its bytes
    may be anything at all. Two things have to be handled or the dead-letter
    write itself fails -- turning a single bad message into a poison pill that
    stalls the partition forever, which is precisely what this table exists to
    prevent:

      * invalid UTF-8, replaced rather than raised on;
      * NUL bytes, which Postgres text columns reject outright and which
        'replace' error handling does not remove.

    Truncated too: this is for diagnosis, not for storing an unbounded payload
    someone accidentally published.
    """
    if raw is None:
        return None
    return raw.decode("utf-8", "replace").replace("\x00", "\\x00")[:limit]


def _decode(msg: Any) -> tuple[Event | None, _Rejected | None]:
    try:
        return decode_message(msg.value()), None
    except EventValidationError as exc:
        return None, _Rejected(
            partition=msg.partition(),
            offset=msg.offset(),
            key=_for_storage(msg.key(), limit=512),
            value=_for_storage(msg.value()),
            error=str(exc),
        )


def consume(
    settings: Settings,
    *,
    max_messages: int | None = None,
    idle_timeout_seconds: float | None = None,
) -> ConsumeSummary:
    """Run the consumer loop until idle, message cap, or Ctrl-C."""
    idle_timeout = (
        settings.consumer_idle_timeout_seconds
        if idle_timeout_seconds is None
        else idle_timeout_seconds
    )

    consumer = build_consumer(settings)
    consumer.subscribe([settings.kafka_topic])

    summary = ConsumeSummary()
    started = time.monotonic()
    last_message_at = time.monotonic()

    # autocommit=True at the connection level; each batch opens its own
    # explicit transaction via conn.transaction().
    conn = psycopg.connect(settings.dsn, autocommit=True)
    batch = _Batch(topic=settings.kafka_topic)

    def flush_if_any() -> None:
        nonlocal batch
        if not len(batch):
            return
        _flush(conn, settings, batch, summary)
        # Offsets move only after the transaction above has committed.
        consumer.commit(offsets=batch.commit_offsets(), asynchronous=False)
        batch = _Batch(topic=settings.kafka_topic)

    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            now = time.monotonic()

            if msg is None:
                if len(batch) and now - batch.opened_at >= settings.consumer_batch_timeout_seconds:
                    flush_if_any()
                if idle_timeout and now - last_message_at >= idle_timeout:
                    summary.stopped_because = "idle"
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            last_message_at = now
            event, rejected = _decode(msg)
            if event is not None:
                batch.events.append((event, msg.partition(), msg.offset()))
            else:
                assert rejected is not None
                batch.rejected.append(rejected)
            batch.note_offset(msg.partition(), msg.offset())

            if len(batch) >= settings.consumer_batch_size:
                flush_if_any()

            if max_messages is not None and summary.messages_read + len(batch) >= max_messages:
                flush_if_any()
                summary.stopped_because = "max-messages"
                break

    except KeyboardInterrupt:
        summary.stopped_because = "interrupted"
        log.info("interrupted -- flushing buffered messages")
    finally:
        try:
            flush_if_any()
        finally:
            # close() commits nothing (auto-commit is off) but does leave the
            # group cleanly, so a restart does not wait out session.timeout.
            consumer.close()
            conn.close()

    summary.elapsed_seconds = time.monotonic() - started
    return summary
