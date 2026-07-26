"""Tests for the event contract and its validation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from clickstream.events import (
    EventValidationError,
    decode_message,
    parse_event,
    parse_timestamp,
)


def make_event(**overrides):
    doc = {
        "event_id": str(uuid4()),
        "event_name": "page_view",
        "anonymous_id": "anon_000001_abcd1234",
        "occurred_at": "2026-07-20T10:15:30+00:00",
        "page": {"path": "/", "title": "Home", "type": "home"},
    }
    doc.update(overrides)
    return doc


class TestParseTimestamp:
    def test_accepts_offset_form(self):
        parsed = parse_timestamp("2026-07-20T10:15:30+00:00")
        assert parsed == datetime(2026, 7, 20, 10, 15, 30, tzinfo=timezone.utc)

    def test_accepts_trailing_z(self):
        assert parse_timestamp("2026-07-20T10:15:30Z") == parse_timestamp(
            "2026-07-20T10:15:30+00:00"
        )

    def test_converts_other_offsets_to_utc(self):
        parsed = parse_timestamp("2026-07-20T12:15:30+02:00")
        assert parsed == datetime(2026, 7, 20, 10, 15, 30, tzinfo=timezone.utc)
        assert parsed.tzinfo == timezone.utc

    def test_naive_timestamp_is_assumed_utc(self):
        # Guessing the host's local zone would silently shift every event.
        parsed = parse_timestamp("2026-07-20T10:15:30")
        assert parsed == datetime(2026, 7, 20, 10, 15, 30, tzinfo=timezone.utc)

    def test_rejects_garbage(self):
        with pytest.raises(EventValidationError, match="ISO-8601"):
            parse_timestamp("last tuesday")


class TestParseEvent:
    def test_parses_a_minimal_page_view(self):
        event = parse_event(make_event())
        assert event.event_name == "page_view"
        assert event.occurred_at.tzinfo == timezone.utc

    def test_partition_key_is_the_visitor(self):
        # Keying by visitor is what keeps one visitor's events on one
        # partition, and therefore ordered.
        event = parse_event(make_event())
        assert event.key() == b"anon_000001_abcd1234"

    def test_value_round_trips_the_payload(self):
        doc = make_event()
        assert json.loads(parse_event(doc).value()) == doc

    @pytest.mark.parametrize("field", ["event_id", "event_name", "anonymous_id", "occurred_at"])
    def test_missing_required_field_is_rejected(self, field):
        doc = make_event()
        del doc[field]
        with pytest.raises(EventValidationError):
            parse_event(doc)

    def test_blank_required_field_is_rejected(self):
        with pytest.raises(EventValidationError, match="anonymous_id"):
            parse_event(make_event(anonymous_id="   "))

    def test_unknown_event_name_is_rejected(self):
        with pytest.raises(EventValidationError, match="unknown event_name"):
            parse_event(make_event(event_name="rage_click"))

    def test_non_uuid_event_id_is_rejected(self):
        with pytest.raises(EventValidationError, match="not a UUID"):
            parse_event(make_event(event_id="evt-1"))

    def test_non_object_message_is_rejected(self):
        with pytest.raises(EventValidationError, match="expected a JSON object"):
            parse_event([1, 2, 3])

    @pytest.mark.parametrize("name", ["product_view", "add_to_cart"])
    def test_product_events_require_a_product(self, name):
        with pytest.raises(EventValidationError, match="product.product_id"):
            parse_event(make_event(event_name=name))

    def test_product_event_with_product_is_accepted(self):
        event = parse_event(
            make_event(
                event_name="product_view",
                product={"product_id": "tent-solo-1p", "price_usd": 229.0},
            )
        )
        assert event.event_name == "product_view"

    def test_purchase_requires_an_order_id(self):
        with pytest.raises(EventValidationError, match="order.order_id"):
            parse_event(make_event(event_name="purchase", order={"revenue_usd": 10.0}))

    def test_purchase_requires_non_negative_revenue(self):
        with pytest.raises(EventValidationError, match="revenue_usd"):
            parse_event(
                make_event(
                    event_name="purchase",
                    order={"order_id": "ord_1", "revenue_usd": -5.0},
                )
            )

    def test_purchase_with_zero_revenue_is_allowed(self):
        # A fully discounted order is real; only negative revenue is a fault.
        event = parse_event(
            make_event(
                event_name="purchase",
                order={"order_id": "ord_1", "revenue_usd": 0},
            )
        )
        assert event.event_name == "purchase"


class TestDecodeMessage:
    def test_decodes_valid_json(self):
        doc = make_event()
        raw = json.dumps(doc).encode("utf-8")
        assert decode_message(raw).event_id == parse_event(doc).event_id

    def test_rejects_tombstone(self):
        with pytest.raises(EventValidationError, match="empty"):
            decode_message(None)

    def test_rejects_malformed_json(self):
        with pytest.raises(EventValidationError, match="valid JSON"):
            decode_message(b"{not json")

    def test_rejects_non_utf8(self):
        with pytest.raises(EventValidationError, match="UTF-8"):
            decode_message(b"\xff\xfe\x00")
