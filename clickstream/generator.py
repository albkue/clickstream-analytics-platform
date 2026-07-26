"""Synthetic clickstream generator.

Produces sessions with the shape real e-commerce traffic has -- a funnel that
leaks at every step, conversion rates that differ by acquisition channel and
device, and visitors who come back later in the week. Without that structure
the marts would be arithmetically correct and analytically meaningless.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from uuid import UUID

from .config import Catalog, Product

# Acquisition channels: (utm_source, utm_medium, share of sessions, how much
# the channel scales the baseline step-through rates). Paid and email traffic
# converts better than social, which is mostly drive-by.
CHANNELS: tuple[tuple[str | None, str | None, str | None, float, float], ...] = (
    (None, None, None, 0.30, 1.05),
    ("google", "organic", None, 0.24, 1.00),
    ("google", "cpc", "summer-gear", 0.16, 1.15),
    ("newsletter", "email", "weekly-digest", 0.10, 1.30),
    ("facebook", "paid_social", "retarget-cart", 0.08, 0.85),
    ("instagram", "social", "creator-tent", 0.07, 0.70),
    ("reddit", "referral", None, 0.05, 0.90),
)

# (device_type, browser, os, share, conversion multiplier). Mobile browses
# heavily and buys reluctantly -- the classic mobile conversion gap.
DEVICES: tuple[tuple[str, str, str, float, float], ...] = (
    ("desktop", "Chrome", "Windows", 0.34, 1.25),
    ("desktop", "Safari", "macOS", 0.14, 1.30),
    ("mobile", "Safari", "iOS", 0.27, 0.80),
    ("mobile", "Chrome", "Android", 0.19, 0.70),
    ("tablet", "Safari", "iPadOS", 0.06, 0.95),
)

COUNTRIES: tuple[tuple[str, float], ...] = (
    ("US", 0.42),
    ("GB", 0.13),
    ("DE", 0.11),
    ("CA", 0.09),
    ("AU", 0.07),
    ("FR", 0.06),
    ("KH", 0.05),
    ("JP", 0.04),
    ("BR", 0.03),
)

SEARCH_TERMS: tuple[str, ...] = (
    "2 person tent",
    "ultralight pack",
    "down sleeping bag",
    "camp stove",
    "sleeping pad r value",
    "waterproof tent",
    "65l backpack",
)

# Baseline probability of taking each funnel step given the previous one.
# Multiplied by the channel and device factors, then clamped to <= 0.95.
STEP_THROUGH = {
    "product_view": 0.62,
    "add_to_cart": 0.34,
    "checkout_start": 0.55,
    "purchase": 0.64,
}


@dataclass(frozen=True)
class Visitor:
    anonymous_id: str
    user_id: str | None
    device_type: str
    browser: str
    os: str
    country: str
    utm_source: str | None
    utm_medium: str | None
    utm_campaign: str | None
    conversion_factor: float


def _weighted(rng: random.Random, options: tuple[tuple[Any, ...], ...], weight_index: int):
    weights = [opt[weight_index] for opt in options]
    return rng.choices(options, weights=weights, k=1)[0]


def _uuid(rng: random.Random) -> UUID:
    """A UUID4 drawn from the seeded RNG, so a seeded run is reproducible."""
    return UUID(int=rng.getrandbits(128), version=4)


def _make_visitor(rng: random.Random, index: int) -> Visitor:
    device_type, browser, os_name, _, device_factor = _weighted(rng, DEVICES, 3)
    source, medium, campaign, _, channel_factor = _weighted(rng, CHANNELS, 3)
    country = _weighted(rng, COUNTRIES, 1)[0]

    # ~22% of visitors are signed in. user_id is what lets the warehouse
    # stitch a person across devices; the rest stay anonymous.
    user_id = f"u_{rng.randrange(10_000, 99_999)}" if rng.random() < 0.22 else None

    return Visitor(
        anonymous_id=f"anon_{index:06d}_{rng.randrange(16**8):08x}",
        user_id=user_id,
        device_type=device_type,
        browser=browser,
        os=os_name,
        country=country,
        utm_source=source,
        utm_medium=medium,
        utm_campaign=campaign,
        conversion_factor=device_factor * channel_factor,
    )


def _base_payload(
    visitor: Visitor,
    event_name: str,
    occurred_at: datetime,
    rng: random.Random,
) -> dict[str, Any]:
    return {
        "event_id": str(_uuid(rng)),
        "event_name": event_name,
        "anonymous_id": visitor.anonymous_id,
        "user_id": visitor.user_id,
        "occurred_at": occurred_at.astimezone(timezone.utc).isoformat(),
        "device": {
            "type": visitor.device_type,
            "browser": visitor.browser,
            "os": visitor.os,
        },
        "geo": {"country": visitor.country},
        "utm": {
            "source": visitor.utm_source,
            "medium": visitor.utm_medium,
            "campaign": visitor.utm_campaign,
        },
    }


def _page_view(
    visitor: Visitor,
    page_path: str,
    page_title: str,
    page_type: str,
    referrer: str | None,
    occurred_at: datetime,
    rng: random.Random,
) -> dict[str, Any]:
    doc = _base_payload(visitor, "page_view", occurred_at, rng)
    doc["page"] = {
        "path": page_path,
        "title": page_title,
        "type": page_type,
        "referrer": referrer,
    }
    return doc


def _product_dict(product: Product, quantity: int = 1) -> dict[str, Any]:
    return {
        "product_id": product.product_id,
        "name": product.name,
        "category": product.category,
        "price_usd": product.price_usd,
        "quantity": quantity,
    }


def _session_start_times(
    rng: random.Random,
    window_start: datetime,
    window_end: datetime,
    count: int,
) -> list[datetime]:
    """Draw session start times with a plausible time-of-day shape.

    Traffic is weighted toward 09:00-22:00 UTC so daily aggregates show a
    diurnal curve rather than a flat line.
    """
    span_seconds = max(int((window_end - window_start).total_seconds()), 1)
    starts: list[datetime] = []
    while len(starts) < count:
        candidate = window_start + timedelta(seconds=rng.randrange(span_seconds))
        hour = candidate.hour
        # Rejection-sample against an hourly weight to bend a uniform draw
        # into a daily curve.
        weight = 0.25 + 0.75 * (1.0 if 9 <= hour < 22 else 0.2)
        if rng.random() <= weight:
            starts.append(candidate)
    return sorted(starts)


def _walk_funnel(
    visitor: Visitor,
    catalog: Catalog,
    start_at: datetime,
    rng: random.Random,
) -> Iterator[dict[str, Any]]:
    """Emit one session's events, leaking visitors at each funnel step."""

    def gap() -> timedelta:
        # Dwell time between hits. Long tail: most hits are quick, a few are
        # someone reading the spec sheet.
        return timedelta(seconds=rng.choice([4, 7, 11, 15, 22, 31, 48, 75, 120]))

    def takes_step(step: str) -> bool:
        probability = min(STEP_THROUGH[step] * visitor.conversion_factor, 0.95)
        return rng.random() < probability

    now = start_at
    referrer = {
        "organic": "https://www.google.com/",
        "cpc": "https://www.google.com/",
        "email": "https://mail.google.com/",
        "paid_social": "https://www.facebook.com/",
        "social": "https://www.instagram.com/",
        "referral": "https://www.reddit.com/",
    }.get(visitor.utm_medium or "", None)

    # --- step 1: landing page view. Every session has one. ---------------
    landing = rng.choice(
        catalog.pages_of_type("home") + catalog.pages_of_type("category")
    )
    yield _page_view(
        visitor, landing.page_path, landing.title, landing.page_type, referrer, now, rng
    )

    # Some visitors search before browsing.
    if rng.random() < 0.28:
        now += gap()
        doc = _base_payload(visitor, "search", now, rng)
        doc["search"] = {
            "query": rng.choice(SEARCH_TERMS),
            "results_count": rng.randrange(0, 24),
        }
        yield doc

        search_page = catalog.pages_of_type("search")
        if search_page:
            now += timedelta(seconds=2)
            yield _page_view(
                visitor,
                search_page[0].page_path,
                search_page[0].title,
                "search",
                landing.page_path,
                now,
                rng,
            )

    # A category page or two of browsing.
    for _ in range(rng.choice([0, 1, 1, 2])):
        category = rng.choice(catalog.pages_of_type("category"))
        now += gap()
        yield _page_view(
            visitor,
            category.page_path,
            category.title,
            "category",
            landing.page_path,
            now,
            rng,
        )

    # --- step 2: product view --------------------------------------------
    if not takes_step("product_view"):
        return

    cart: list[dict[str, Any]] = []
    products_seen = rng.sample(
        catalog.products, k=min(rng.choice([1, 1, 2, 3]), len(catalog.products))
    )

    for product in products_seen:
        now += gap()
        product_page = f"/p/{product.product_id}"
        yield _page_view(
            visitor, product_page, product.name, "product", landing.page_path, now, rng
        )

        now += timedelta(seconds=1)
        doc = _base_payload(visitor, "product_view", now, rng)
        doc["page"] = {"path": product_page, "title": product.name, "type": "product"}
        doc["product"] = _product_dict(product)
        yield doc

        # --- step 3: add to cart -----------------------------------------
        if takes_step("add_to_cart"):
            quantity = rng.choice([1, 1, 1, 2])
            now += gap()
            doc = _base_payload(visitor, "add_to_cart", now, rng)
            doc["page"] = {"path": product_page, "title": product.name, "type": "product"}
            doc["product"] = _product_dict(product, quantity)
            yield doc
            cart.append(_product_dict(product, quantity))

    if not cart:
        return

    # Cart page view before checkout.
    now += gap()
    yield _page_view(visitor, "/cart", "Your cart", "cart", None, now, rng)

    # --- step 4: checkout start -------------------------------------------
    if not takes_step("checkout_start"):
        return

    now += gap()
    yield _page_view(visitor, "/checkout", "Checkout", "checkout", "/cart", now, rng)
    now += timedelta(seconds=2)
    cart_value = round(sum(i["price_usd"] * i["quantity"] for i in cart), 2)
    doc = _base_payload(visitor, "checkout_start", now, rng)
    doc["page"] = {"path": "/checkout", "title": "Checkout", "type": "checkout"}
    doc["cart"] = {"items": cart, "item_count": len(cart), "value_usd": cart_value}
    yield doc

    # --- step 5: purchase --------------------------------------------------
    if not takes_step("purchase"):
        return

    now += gap()
    shipping = round(rng.choice([0.0, 0.0, 5.95, 12.5]), 2)
    doc = _base_payload(visitor, "purchase", now, rng)
    doc["page"] = {
        "path": "/checkout/confirm",
        "title": "Order confirmed",
        "type": "confirmation",
    }
    doc["order"] = {
        "order_id": f"ord_{_uuid(rng).hex[:12]}",
        "items": cart,
        "item_count": sum(int(i["quantity"]) for i in cart),
        "revenue_usd": round(cart_value + shipping, 2),
        "shipping_usd": shipping,
        "currency": "USD",
    }
    yield doc

    now += timedelta(seconds=3)
    yield _page_view(
        visitor,
        "/checkout/confirm",
        "Order confirmed",
        "confirmation",
        "/checkout",
        now,
        rng,
    )


def generate_events(
    catalog: Catalog,
    *,
    sessions: int,
    window_start: datetime,
    window_end: datetime,
    seed: int | None = None,
    session_timeout_minutes: int = 30,
    returning_visitor_rate: float = 0.30,
) -> list[dict[str, Any]]:
    """Generate `sessions` sessions' worth of events, ordered by time.

    Returns payload dicts ready to be serialised onto the topic. Emitting
    them time-ordered means the topic looks like a real stream replayed at
    speed rather than a shuffled dump.
    """
    if sessions <= 0:
        raise ValueError(f"sessions must be positive, got {sessions}")
    if window_end <= window_start:
        raise ValueError("window_end must be after window_start")

    rng = random.Random(seed)

    # Fewer visitors than sessions gives us returning visitors, which is what
    # makes gap-based sessionization observable: the same anonymous_id shows
    # up twice with hours between.
    visitor_count = max(1, int(sessions * (1.0 - returning_visitor_rate)))
    visitors = [_make_visitor(rng, i) for i in range(visitor_count)]

    starts = _session_start_times(rng, window_start, window_end, sessions)

    # Track when each visitor's previous session ENDED, not when it started.
    # The gap the warehouse measures is between consecutive events, so a long
    # session followed by a nominally-distant next visit can still fall inside
    # the timeout and correctly collapse into one -- which would leave fewer
    # sessions than were asked for.
    min_gap = timedelta(minutes=session_timeout_minutes * 2)
    last_event_at: dict[str, datetime] = {}

    events: list[dict[str, Any]] = []
    for start_at in starts:
        candidates = [
            v
            for v in visitors
            if v.anonymous_id not in last_event_at
            or start_at - last_event_at[v.anonymous_id] >= min_gap
        ]
        visitor = rng.choice(candidates or visitors)

        previous_end = last_event_at.get(visitor.anonymous_id)
        if previous_end is not None and start_at - previous_end < min_gap:
            # No visitor was free at this instant; shift the session forward
            # rather than emit two that would merge into one.
            start_at = previous_end + min_gap

        session_events = list(_walk_funnel(visitor, catalog, start_at, rng))
        # _walk_funnel yields in increasing time order, so the last event is
        # the session's end.
        last_event_at[visitor.anonymous_id] = datetime.fromisoformat(
            session_events[-1]["occurred_at"]
        )
        events.extend(session_events)

    # All timestamps are UTC ISO-8601 with the same offset, so lexicographic
    # order is chronological order.
    events.sort(key=lambda e: e["occurred_at"])
    return events
