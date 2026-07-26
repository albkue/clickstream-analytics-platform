"""Tests for settings and catalog parsing."""

from __future__ import annotations

import json

import pytest

from clickstream.config import EVENT_NAMES, FUNNEL_STEPS, load_catalog, load_settings

VALID_CATALOG = {
    "pages": [
        {"page_path": "/", "page_type": "home", "title": "Home"},
        {"page_path": "/p/x", "page_type": "product", "title": "X"},
    ],
    "products": [
        {"product_id": "x", "name": "X", "category": "c", "price_usd": 10.0}
    ],
}


def write_catalog(tmp_path, doc):
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


class TestFunnelDefinition:
    def test_funnel_steps_are_all_valid_event_names(self):
        # The SQL funnel model ranks against FUNNEL_STEPS, so a step that the
        # consumer would reject could never be reached.
        assert set(FUNNEL_STEPS) <= EVENT_NAMES

    def test_funnel_starts_at_page_view_and_ends_at_purchase(self):
        assert FUNNEL_STEPS[0] == "page_view"
        assert FUNNEL_STEPS[-1] == "purchase"


class TestLoadSettings:
    def test_reads_environment(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "db.example")
        monkeypatch.setenv("POSTGRES_PORT", "6000")
        monkeypatch.setenv("SESSION_TIMEOUT_MINUTES", "45")
        settings = load_settings()
        assert settings.pg_host == "db.example"
        assert settings.pg_port == 6000
        assert settings.session_timeout_minutes == 45

    def test_dsn_is_assembled_from_parts(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "h")
        monkeypatch.setenv("POSTGRES_PORT", "1")
        monkeypatch.setenv("POSTGRES_USER", "u")
        monkeypatch.setenv("POSTGRES_PASSWORD", "p")
        monkeypatch.setenv("POSTGRES_DB", "d")
        assert load_settings().dsn == "host=h port=1 user=u password=p dbname=d"

    def test_blank_value_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("SESSION_TIMEOUT_MINUTES", "")
        assert load_settings().session_timeout_minutes == 30

    def test_non_integer_is_rejected(self, monkeypatch):
        monkeypatch.setenv("CONSUMER_BATCH_SIZE", "lots")
        with pytest.raises(ValueError, match="must be an integer"):
            load_settings()

    def test_zero_session_timeout_is_rejected(self, monkeypatch):
        # A zero-minute gap would make every single event its own session.
        monkeypatch.setenv("SESSION_TIMEOUT_MINUTES", "0")
        with pytest.raises(ValueError, match="must be positive"):
            load_settings()

    def test_zero_batch_size_is_rejected(self, monkeypatch):
        monkeypatch.setenv("CONSUMER_BATCH_SIZE", "0")
        with pytest.raises(ValueError, match="must be positive"):
            load_settings()

    def test_generator_seed_is_optional(self, monkeypatch):
        monkeypatch.setenv("GENERATOR_SEED", "")
        assert load_settings().generator_seed is None
        monkeypatch.setenv("GENERATOR_SEED", "7")
        assert load_settings().generator_seed == 7


class TestLoadCatalog:
    def test_parses_a_valid_catalog(self, tmp_path):
        catalog = load_catalog(write_catalog(tmp_path, VALID_CATALOG))
        assert len(catalog.pages) == 2
        assert catalog.products[0].price_usd == 10.0

    def test_pages_of_type_filters(self, tmp_path):
        catalog = load_catalog(write_catalog(tmp_path, VALID_CATALOG))
        assert [p.page_path for p in catalog.pages_of_type("home")] == ["/"]

    def test_missing_file_is_reported(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_catalog(tmp_path / "nope.json")

    def test_duplicate_page_path_is_rejected(self, tmp_path):
        doc = json.loads(json.dumps(VALID_CATALOG))
        doc["pages"].append(doc["pages"][0])
        with pytest.raises(ValueError, match="duplicate page_path"):
            load_catalog(write_catalog(tmp_path, doc))

    def test_duplicate_product_id_is_rejected(self, tmp_path):
        doc = json.loads(json.dumps(VALID_CATALOG))
        doc["products"].append(doc["products"][0])
        with pytest.raises(ValueError, match="duplicate product_id"):
            load_catalog(write_catalog(tmp_path, doc))

    def test_relative_page_path_is_rejected(self, tmp_path):
        doc = json.loads(json.dumps(VALID_CATALOG))
        doc["pages"][0]["page_path"] = "home"
        with pytest.raises(ValueError, match="must start with"):
            load_catalog(write_catalog(tmp_path, doc))

    def test_missing_field_names_the_field(self, tmp_path):
        doc = json.loads(json.dumps(VALID_CATALOG))
        del doc["products"][0]["price_usd"]
        with pytest.raises(ValueError, match="price_usd"):
            load_catalog(write_catalog(tmp_path, doc))

    def test_non_positive_price_is_rejected(self, tmp_path):
        doc = json.loads(json.dumps(VALID_CATALOG))
        doc["products"][0]["price_usd"] = 0
        with pytest.raises(ValueError, match="must be positive"):
            load_catalog(write_catalog(tmp_path, doc))

    def test_catalog_without_a_product_page_is_rejected(self, tmp_path):
        # The generator needs somewhere to land and something to sell, or it
        # emits a stream with no funnel in it.
        doc = json.loads(json.dumps(VALID_CATALOG))
        doc["pages"] = [doc["pages"][0]]
        with pytest.raises(ValueError, match="'product' page"):
            load_catalog(write_catalog(tmp_path, doc))

    def test_empty_products_is_rejected(self, tmp_path):
        doc = json.loads(json.dumps(VALID_CATALOG))
        doc["products"] = []
        with pytest.raises(ValueError, match="'products' array"):
            load_catalog(write_catalog(tmp_path, doc))
