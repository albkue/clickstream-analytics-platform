-- Clickstream warehouse schema.
--
-- Layered:  raw (as-landed Kafka payloads)
--        -> stg (typed/cleaned, built by the model runner)
--        -> mart (dims, facts, aggregates, built by the model runner)
--        -> meta (ingest + transform run log)
--
-- This file owns raw and meta only. stg and mart are (re)built from
-- models/**.sql by `python -m clickstream transform`, so their DDL lives
-- there rather than here -- the runner drops and recreates those objects.
--
-- Every statement is idempotent so the file can be re-applied at any time.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS meta;

-- ---------------------------------------------------------------- meta ----

-- One row per consumer flush. A batch is the unit that makes ingestion
-- at-least-once: rows are written and this row is marked committed inside
-- one transaction, and only then are Kafka offsets committed.
CREATE TABLE IF NOT EXISTS meta.ingest_batches (
    batch_id       BIGSERIAL PRIMARY KEY,
    consumer_group TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT        NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'committed', 'failed')),
    messages_read  INTEGER     NOT NULL DEFAULT 0,
    rows_inserted  INTEGER     NOT NULL DEFAULT 0,
    rows_duplicate INTEGER     NOT NULL DEFAULT 0,
    rows_rejected  INTEGER     NOT NULL DEFAULT 0,
    -- {"<partition>": <last offset read>} -- lineage for replay/debugging.
    offsets        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error          TEXT
);

CREATE INDEX IF NOT EXISTS ix_ingest_batches_started_at
    ON meta.ingest_batches (started_at DESC);

-- One row per model per `transform` invocation.
CREATE TABLE IF NOT EXISTS meta.model_runs (
    model_run_id  BIGSERIAL PRIMARY KEY,
    run_id        UUID        NOT NULL,
    model_name    TEXT        NOT NULL,
    materialized  TEXT        NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT        NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running', 'success', 'skipped', 'failed')),
    rows_affected BIGINT,
    full_refresh  BOOLEAN     NOT NULL DEFAULT false,
    error         TEXT
);

CREATE INDEX IF NOT EXISTS ix_model_runs_run_id
    ON meta.model_runs (run_id, started_at);

-- One row per schema test per `test` invocation.
CREATE TABLE IF NOT EXISTS meta.test_results (
    test_result_id BIGSERIAL PRIMARY KEY,
    run_id         UUID        NOT NULL,
    model_name     TEXT        NOT NULL,
    column_name    TEXT,
    test_name      TEXT        NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('pass', 'fail', 'error')),
    failing_rows   BIGINT,
    executed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    error          TEXT
);

CREATE INDEX IF NOT EXISTS ix_test_results_run_id
    ON meta.test_results (run_id, model_name);

-- ----------------------------------------------------------------- raw ----

-- Landing table for validated events.
--
-- event_id is the producer-assigned UUID and the primary key, which is what
-- makes redelivery harmless: the consumer inserts ON CONFLICT DO NOTHING, so
-- at-least-once delivery from Kafka lands exactly once here.
--
-- event_name / anonymous_id / occurred_at are promoted out of the payload
-- because every downstream model filters or partitions on them; the full
-- original message is still kept in payload.
CREATE TABLE IF NOT EXISTS raw.events (
    event_id        UUID        PRIMARY KEY,
    event_name      TEXT        NOT NULL,
    anonymous_id    TEXT        NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    payload         JSONB       NOT NULL,
    kafka_topic     TEXT        NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    batch_id        BIGINT      REFERENCES meta.ingest_batches (batch_id),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Drives the incremental staging model's watermark.
CREATE INDEX IF NOT EXISTS ix_raw_events_ingested_at
    ON raw.events (ingested_at);

-- Sessionization reads events per visitor in time order.
CREATE INDEX IF NOT EXISTS ix_raw_events_visitor_time
    ON raw.events (anonymous_id, occurred_at);

-- Messages that could not be parsed or failed validation. Keeping them out
-- of raw.events means one malformed producer cannot stall the consumer, and
-- keeping them at all means the loss is visible instead of silent.
CREATE TABLE IF NOT EXISTS raw.events_dead_letter (
    dead_letter_id  BIGSERIAL PRIMARY KEY,
    batch_id        BIGINT      REFERENCES meta.ingest_batches (batch_id),
    kafka_topic     TEXT        NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    message_key     TEXT,
    -- Stored as text, not jsonb: the whole point is that it may not be valid
    -- JSON, and a jsonb column would reject exactly the rows we need to keep.
    message_value   TEXT,
    error           TEXT        NOT NULL,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dead_letter_rejected_at
    ON raw.events_dead_letter (rejected_at DESC);
