"""Configuration: environment settings and the site/product catalog."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# The conversion funnel, in order. This is the one place the step sequence is
# defined: the generator walks it, the SQL models rank against it, and the
# funnel report reads it back out of the warehouse.
FUNNEL_STEPS: tuple[str, ...] = (
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout_start",
    "purchase",
)

# Every event name the pipeline accepts. Anything else is dead-lettered.
# `search` is tracked but sits outside the funnel: it is a browsing signal,
# not a step on the path to purchase.
EVENT_NAMES: frozenset[str] = frozenset(FUNNEL_STEPS) | {"search"}


@dataclass(frozen=True)
class Page:
    page_path: str
    page_type: str
    title: str


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str
    price_usd: float


@dataclass(frozen=True)
class Catalog:
    pages: list[Page]
    products: list[Product]

    def pages_of_type(self, page_type: str) -> list[Page]:
        return [p for p in self.pages if p.page_type == page_type]


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_database: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_consumer_group: str
    kafka_topic_partitions: int
    consumer_batch_size: int
    consumer_batch_timeout_seconds: float
    consumer_idle_timeout_seconds: float
    session_timeout_minutes: int
    models_dir: Path
    catalog_file: Path
    generator_seed: int | None

    @property
    def dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} user={self.pg_user} "
            f"password={self.pg_password} dbname={self.pg_database}"
        )


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def _path_env(name: str, default: str) -> Path:
    path = Path(os.getenv(name) or default)
    return path if path.is_absolute() else PROJECT_ROOT / path


def load_settings() -> Settings:
    """Read .env (if present) plus the real environment into a Settings object."""
    load_dotenv(PROJECT_ROOT / ".env")

    seed_raw = os.getenv("GENERATOR_SEED", "").strip()

    session_timeout = _int_env("SESSION_TIMEOUT_MINUTES", 30)
    if session_timeout <= 0:
        raise ValueError(
            f"SESSION_TIMEOUT_MINUTES must be positive, got {session_timeout}"
        )

    batch_size = _int_env("CONSUMER_BATCH_SIZE", 500)
    if batch_size <= 0:
        raise ValueError(f"CONSUMER_BATCH_SIZE must be positive, got {batch_size}")

    return Settings(
        pg_host=os.getenv("POSTGRES_HOST", "localhost"),
        pg_port=_int_env("POSTGRES_PORT", 5435),
        pg_user=os.getenv("POSTGRES_USER", "clickstream"),
        pg_password=os.getenv("POSTGRES_PASSWORD", "clickstream"),
        pg_database=os.getenv("POSTGRES_DB", "clickstream"),
        kafka_bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"
        ),
        kafka_topic=os.getenv("KAFKA_TOPIC", "clickstream.events"),
        kafka_consumer_group=os.getenv(
            "KAFKA_CONSUMER_GROUP", "clickstream-warehouse-loader"
        ),
        kafka_topic_partitions=_int_env("KAFKA_TOPIC_PARTITIONS", 6),
        consumer_batch_size=batch_size,
        consumer_batch_timeout_seconds=_float_env(
            "CONSUMER_BATCH_TIMEOUT_SECONDS", 5.0
        ),
        consumer_idle_timeout_seconds=_float_env(
            "CONSUMER_IDLE_TIMEOUT_SECONDS", 0.0
        ),
        session_timeout_minutes=session_timeout,
        models_dir=_path_env("MODELS_DIR", "models"),
        catalog_file=_path_env("CATALOG_FILE", "config/catalog.json"),
        generator_seed=int(seed_raw) if seed_raw else None,
    )


def load_catalog(path: Path) -> Catalog:
    """Parse the site/product catalog, failing loudly on a malformed entry."""
    if not path.exists():
        raise FileNotFoundError(f"Catalog file not found: {path}")

    doc = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ValueError(f"{path} must contain a JSON object")

    pages = _parse_pages(path, doc.get("pages"))
    products = _parse_products(path, doc.get("products"))

    # The generator needs somewhere to land visitors and something to sell;
    # without either, it would emit a stream with no funnel in it.
    for page_type in ("home", "product"):
        if not any(p.page_type == page_type for p in pages):
            raise ValueError(f"{path} must define at least one {page_type!r} page")

    return Catalog(pages=pages, products=products)


def _parse_pages(path: Path, entries: object) -> list[Page]:
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"{path} must contain a non-empty 'pages' array")

    pages: list[Page] = []
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        missing = {"page_path", "page_type", "title"} - entry.keys()
        if missing:
            raise ValueError(f"{path} pages[{i}] is missing {sorted(missing)}")

        page_path = entry["page_path"]
        if not page_path.startswith("/"):
            raise ValueError(f"{path} pages[{i}]: page_path must start with '/'")
        if page_path in seen:
            raise ValueError(f"{path} has duplicate page_path {page_path!r}")
        seen.add(page_path)

        pages.append(
            Page(
                page_path=page_path,
                page_type=entry["page_type"],
                title=entry["title"],
            )
        )
    return pages


def _parse_products(path: Path, entries: object) -> list[Product]:
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"{path} must contain a non-empty 'products' array")

    products: list[Product] = []
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        missing = {"product_id", "name", "category", "price_usd"} - entry.keys()
        if missing:
            raise ValueError(f"{path} products[{i}] is missing {sorted(missing)}")

        product_id = entry["product_id"]
        if product_id in seen:
            raise ValueError(f"{path} has duplicate product_id {product_id!r}")
        seen.add(product_id)

        price = float(entry["price_usd"])
        if price <= 0:
            raise ValueError(f"{path} products[{i}]: price_usd must be positive")

        products.append(
            Product(
                product_id=product_id,
                name=entry["name"],
                category=entry["category"],
                price_usd=price,
            )
        )
    return products
