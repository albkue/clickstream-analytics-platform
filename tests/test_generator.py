"""Tests for the synthetic event generator.

These also pin down the contract the SQL sessionizer relies on: a session
boundary is a gap longer than the timeout, and the funnel only ever narrows.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from clickstream.config import FUNNEL_STEPS, PROJECT_ROOT, load_catalog
from clickstream.events import parse_event

WINDOW_END = datetime(2026, 7, 20, tzinfo=timezone.utc)
WINDOW_START = WINDOW_END - timedelta(days=5)
TIMEOUT_MINUTES = 30


@pytest.fixture(scope="module")
def catalog():
    return load_catalog(PROJECT_ROOT / "config" / "catalog.json")


@pytest.fixture(scope="module")
def events(catalog):
    from clickstream.generator import generate_events

    return generate_events(
        catalog,
        sessions=300,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        seed=1234,
        session_timeout_minutes=TIMEOUT_MINUTES,
    )


def sessionize(events, timeout_minutes=TIMEOUT_MINUTES):
    """Reference implementation of the gap rule the SQL model implements."""
    by_visitor = defaultdict(list)
    for event in events:
        by_visitor[event["anonymous_id"]].append(
            datetime.fromisoformat(event["occurred_at"])
        )

    gap = timedelta(minutes=timeout_minutes)
    sessions = 0
    per_visitor = {}
    for visitor, times in by_visitor.items():
        times.sort()
        count = 1
        for previous, current in zip(times, times[1:]):
            if current - previous > gap:
                count += 1
        per_visitor[visitor] = count
        sessions += count
    return sessions, per_visitor


class TestGeneratorContract:
    def test_rejects_non_positive_sessions(self, catalog):
        from clickstream.generator import generate_events

        with pytest.raises(ValueError, match="sessions must be positive"):
            generate_events(
                catalog, sessions=0, window_start=WINDOW_START, window_end=WINDOW_END
            )

    def test_rejects_an_inverted_window(self, catalog):
        from clickstream.generator import generate_events

        with pytest.raises(ValueError, match="must be after"):
            generate_events(
                catalog, sessions=5, window_start=WINDOW_END, window_end=WINDOW_START
            )

    def test_is_deterministic_under_a_seed(self, catalog):
        from clickstream.generator import generate_events

        kwargs = dict(
            sessions=40,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            seed=99,
        )
        assert generate_events(catalog, **kwargs) == generate_events(catalog, **kwargs)

    def test_different_seeds_differ(self, catalog):
        from clickstream.generator import generate_events

        kwargs = dict(sessions=40, window_start=WINDOW_START, window_end=WINDOW_END)
        assert generate_events(catalog, seed=1, **kwargs) != generate_events(
            catalog, seed=2, **kwargs
        )


class TestGeneratedEvents:
    def test_every_event_passes_validation(self, events):
        # The producer validates with the same parser, so a generator that
        # emitted invalid events would fail at publish time.
        for event in events:
            parse_event(event)

    def test_events_are_time_ordered(self, events):
        times = [e["occurred_at"] for e in events]
        assert times == sorted(times)

    def test_events_start_within_the_window(self, events):
        first = datetime.fromisoformat(events[0]["occurred_at"])
        assert first >= WINDOW_START

    def test_every_event_id_is_unique(self, events):
        ids = [e["event_id"] for e in events]
        assert len(set(ids)) == len(ids)

    def test_all_funnel_steps_appear(self, events):
        seen = {e["event_name"] for e in events}
        assert set(FUNNEL_STEPS) <= seen

    def test_purchases_carry_positive_revenue(self, events):
        purchases = [e for e in events if e["event_name"] == "purchase"]
        assert purchases
        for event in purchases:
            assert event["order"]["revenue_usd"] > 0
            assert event["order"]["items"]

    def test_purchase_revenue_covers_the_cart(self, events):
        for event in events:
            if event["event_name"] != "purchase":
                continue
            items = event["order"]["items"]
            goods = sum(i["price_usd"] * i["quantity"] for i in items)
            shipping = event["order"]["shipping_usd"]
            assert event["order"]["revenue_usd"] == pytest.approx(
                goods + shipping, abs=0.01
            )


class TestFunnelShape:
    def test_funnel_narrows_at_every_step(self, events):
        # The defining property of a funnel. If this ever inverts, the
        # generated data cannot exercise the conversion models meaningfully.
        counts = [
            len({e["anonymous_id"] for e in events if e["event_name"] == step})
            for step in FUNNEL_STEPS
        ]
        assert counts == sorted(counts, reverse=True)
        assert counts[-1] > 0

    def test_not_everyone_converts(self, events):
        visitors = {e["anonymous_id"] for e in events}
        buyers = {e["anonymous_id"] for e in events if e["event_name"] == "purchase"}
        assert 0 < len(buyers) < len(visitors)


class TestSessionization:
    def test_gap_rule_recovers_the_requested_session_count(self, catalog):
        from clickstream.generator import generate_events

        for requested in (50, 200):
            events = generate_events(
                catalog,
                sessions=requested,
                window_start=WINDOW_START,
                window_end=WINDOW_END,
                seed=7,
                session_timeout_minutes=TIMEOUT_MINUTES,
            )
            found, _ = sessionize(events)
            assert found == requested

    def test_some_visitors_return(self, events):
        # Without returning visitors, gap-based sessionization would be
        # indistinguishable from grouping by anonymous_id.
        _, per_visitor = sessionize(events)
        assert max(per_visitor.values()) > 1

    def test_no_gap_lands_near_the_timeout_threshold(self, events):
        """Every consecutive gap is unambiguously intra- or inter-session.

        Gaps within a session are minutes; gaps between them are at least
        twice the timeout. Nothing sits near the 30-minute boundary, so the
        SQL model's cut and the generator's notion of a session cannot
        disagree because of a borderline value.
        """
        by_visitor = defaultdict(list)
        for event in events:
            by_visitor[event["anonymous_id"]].append(
                datetime.fromisoformat(event["occurred_at"])
            )

        timeout = timedelta(minutes=TIMEOUT_MINUTES)
        for visitor, times in by_visitor.items():
            times.sort()
            for previous, current in zip(times, times[1:]):
                delta = current - previous
                assert delta <= timeout or delta >= 2 * timeout, (
                    f"{visitor} has an ambiguous {delta} gap near the "
                    f"{timeout} session timeout"
                )

    def test_session_boundaries_agree_with_the_requested_timeout(self, events):
        """Re-cutting with a longer timeout must not create more sessions."""
        at_30, _ = sessionize(events, timeout_minutes=30)
        at_120, _ = sessionize(events, timeout_minutes=120)
        assert at_120 <= at_30
