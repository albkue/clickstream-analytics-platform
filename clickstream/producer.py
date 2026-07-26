"""Publish clickstream events onto the Kafka topic."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import Settings
from .events import parse_event

log = logging.getLogger(__name__)


@dataclass
class PublishSummary:
    produced: int = 0
    failed: int = 0
    invalid: int = 0
    elapsed_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.failed == 0 and self.invalid == 0

    @property
    def rate(self) -> float:
        return self.produced / self.elapsed_seconds if self.elapsed_seconds else 0.0


def ensure_topic(settings: Settings, timeout: float = 30.0) -> bool:
    """Create the topic with the configured partition count if it is missing.

    Relying on broker auto-creation would give the topic whatever
    num.partitions the broker defaults to. Partition count is load-bearing
    here (it caps consumer parallelism and fixes the per-visitor ordering
    guarantee), so it is set explicitly.

    Returns True if the topic was created, False if it already existed.
    """
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})

    metadata = admin.list_topics(timeout=timeout)
    if settings.kafka_topic in metadata.topics:
        existing = len(metadata.topics[settings.kafka_topic].partitions)
        if existing != settings.kafka_topic_partitions:
            # Partitions can be added but never removed, so this is a warning
            # rather than an automatic (and possibly destructive) change.
            log.warning(
                "topic %s has %d partitions, config says %d -- leaving as is",
                settings.kafka_topic,
                existing,
                settings.kafka_topic_partitions,
            )
        return False

    new_topic = NewTopic(
        settings.kafka_topic,
        num_partitions=settings.kafka_topic_partitions,
        replication_factor=1,
    )
    for topic, future in admin.create_topics([new_topic]).items():
        try:
            future.result(timeout=timeout)
            log.info(
                "created topic %s with %d partitions",
                topic,
                settings.kafka_topic_partitions,
            )
        except KafkaException as exc:
            # A concurrent producer may have won the race; that is fine.
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                return False
            raise
    return True


def build_producer(settings: Settings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "clickstream-producer",
            # Wait for all in-sync replicas before considering a write done.
            # Single-broker here, but it is the setting you want in prod and
            # costs nothing to keep correct.
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
            # Small linger batches events without adding perceptible latency.
            "linger.ms": 10,
            "retries": 5,
        }
    )


def publish(
    settings: Settings,
    events: Iterable[dict[str, Any]],
    *,
    rate_per_second: float = 0.0,
    progress_every: int = 1000,
) -> PublishSummary:
    """Validate and publish events.

    Events are validated with the same parser the consumer uses, so a bad
    payload fails here at the producer instead of silently becoming a
    dead-letter row later.

    rate_per_second > 0 paces the stream to simulate live traffic; 0 sends as
    fast as the broker accepts.
    """
    producer = build_producer(settings)
    summary = PublishSummary()

    def on_delivery(err: Any, msg: Any) -> None:
        if err is None:
            summary.produced += 1
            return
        summary.failed += 1
        if len(summary.errors) < 10:
            summary.errors.append(str(err))

    interval = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
    started = time.monotonic()
    next_send = started
    queued = 0

    for doc in events:
        try:
            event = parse_event(doc)
        except ValueError as exc:
            summary.invalid += 1
            if len(summary.errors) < 10:
                summary.errors.append(f"invalid event: {exc}")
            continue

        if interval:
            sleep_for = next_send - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            next_send += interval

        while True:
            try:
                producer.produce(
                    settings.kafka_topic,
                    key=event.key(),
                    value=event.value(),
                    # Kafka wants milliseconds since epoch. Setting it from
                    # occurred_at (not now()) keeps retention and any future
                    # stream-time processing aligned with event time.
                    timestamp=int(event.occurred_at.timestamp() * 1000),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                # Local queue is full: serve delivery callbacks to drain it.
                producer.poll(0.5)

        queued += 1
        if queued % progress_every == 0:
            producer.poll(0)
            log.info("queued %d events", queued)

    remaining = producer.flush(timeout=60.0)
    if remaining:
        summary.failed += remaining
        summary.errors.append(f"{remaining} message(s) still unflushed after 60s")

    summary.elapsed_seconds = time.monotonic() - started
    return summary


def publish_json_lines(
    settings: Settings, path: str, **kwargs: Any
) -> PublishSummary:
    """Publish events from a newline-delimited JSON file."""

    def _read() -> Iterable[dict[str, Any]]:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    yield json.loads(line)

    return publish(settings, _read(), **kwargs)
