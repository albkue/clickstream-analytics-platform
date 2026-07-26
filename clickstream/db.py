"""Database connection helpers and schema migration."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

import psycopg

from .config import PROJECT_ROOT, Settings

log = logging.getLogger(__name__)

SQL_DIR = PROJECT_ROOT / "sql"


@contextmanager
def connect(settings: Settings) -> Iterator[psycopg.Connection]:
    """Open a connection, committing on success and rolling back on error."""
    conn = psycopg.connect(settings.dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def apply_migrations(settings: Settings) -> list[str]:
    """Apply every sql/*.sql file in name order. All statements are idempotent."""
    files = sorted(SQL_DIR.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No .sql files found in {SQL_DIR}")

    with connect(settings) as conn:
        for path in files:
            log.info("applying %s", path.name)
            conn.execute(path.read_text(encoding="utf-8"))
    return [p.name for p in files]
