"""The event contract shared by the producer and the consumer.

One deliberate omission: an event carries no session id. Client-supplied
session ids drift (cookie loss, multiple tabs, clock skew) and cannot be
re-cut once written. Sessions here are derived in the warehouse from the
inactivity gap instead, so changing SESSION_TIMEOUT_MINUTES and running
`transform --full-refresh` re-sessionizes all of history.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from .config import EVENT_NAMES


class EventValidationError(ValueError):
    """A message was well-formed JSON but is not a usable event."""


@dataclass(frozen=True)
class Event:
    event_id: UUID
    event_name: str
    anonymous_id: str
    occurred_at: datetime
    payload: dict[str, Any] = field(repr=False)

    def key(self) -> bytes:
        """Kafka partition key.

        Keying by visitor rather than by event id keeps one visitor's events
        on one partition and therefore in order, which is what lets the
        consumer and the sessionizer trust `occurred_at` ordering per visitor.
        """
        return self.anonymous_id.encode("utf-8")

    def value(self) -> bytes:
        return json.dumps(self.payload, separators=(",", ":")).encode("utf-8")


def _require(doc: dict[str, Any], key: str) -> Any:
    value = doc.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise EventValidationError(f"missing required field {key!r}")
    return value


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp, normalising to aware UTC.

    A naive timestamp is treated as UTC rather than rejected: producers in
    the wild send both, and guessing the host's local zone would silently
    shift every event.
    """
    text = value.strip()
    # fromisoformat gained 'Z' support in 3.11; normalise anyway so the
    # accepted format does not depend on the interpreter version.
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise EventValidationError(
            f"occurred_at is not ISO-8601: {value!r}"
        ) from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_event(doc: Any) -> Event:
    """Validate a decoded JSON message into an Event.

    Raises EventValidationError on anything the warehouse could not model.
    The consumer turns that into a dead-letter row rather than a crash.
    """
    if not isinstance(doc, dict):
        raise EventValidationError(f"expected a JSON object, got {type(doc).__name__}")

    event_name = _require(doc, "event_name")
    if event_name not in EVENT_NAMES:
        raise EventValidationError(f"unknown event_name {event_name!r}")

    raw_id = _require(doc, "event_id")
    try:
        event_id = UUID(str(raw_id))
    except ValueError as exc:
        raise EventValidationError(f"event_id is not a UUID: {raw_id!r}") from exc

    anonymous_id = str(_require(doc, "anonymous_id"))
    occurred_at = parse_timestamp(str(_require(doc, "occurred_at")))

    # A product event with no product cannot join to dim_product, so it is
    # rejected at the edge instead of becoming a null in the mart.
    if event_name in {"product_view", "add_to_cart"}:
        product = doc.get("product")
        if not isinstance(product, dict) or not product.get("product_id"):
            raise EventValidationError(f"{event_name} requires product.product_id")

    if event_name == "purchase":
        order = doc.get("order")
        if not isinstance(order, dict) or not order.get("order_id"):
            raise EventValidationError("purchase requires order.order_id")
        revenue = order.get("revenue_usd")
        if not isinstance(revenue, (int, float)) or revenue < 0:
            raise EventValidationError(
                f"purchase requires a non-negative order.revenue_usd, got {revenue!r}"
            )

    return Event(
        event_id=event_id,
        event_name=event_name,
        anonymous_id=anonymous_id,
        occurred_at=occurred_at,
        payload=doc,
    )


def decode_message(value: bytes | None) -> Event:
    """Decode a Kafka message value into an Event."""
    if value is None:
        raise EventValidationError("message value is empty (tombstone?)")
    try:
        doc = json.loads(value.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise EventValidationError(f"message value is not UTF-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise EventValidationError(f"message value is not valid JSON: {exc}") from exc
    return parse_event(doc)
