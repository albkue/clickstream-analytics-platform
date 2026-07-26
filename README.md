# Clickstream Analytics Platform

A real-time streaming pipeline that ingests e-commerce clickstream events
through Kafka, lands them in Postgres, and models them into a warehouse with
dbt-style transformations — page views, derived sessions, and conversion
funnels.

```
 generator ──▶ Kafka topic ──▶ consumer ──▶ raw.events ──▶ models/ ──▶ mart.*
              clickstream.events           (Postgres)      (DAG)      funnels
              6 partitions,                                           sessions
              keyed by visitor                                        traffic
```

Everything runs locally against `docker compose`: a single-node Kafka in KRaft
mode (no ZooKeeper) and Postgres 16.

---

## Quick start

```bash
docker compose up -d
```

```bash
python -m venv .venv && ./.venv/Scripts/pip install -r requirements.txt
```

```bash
cp .env.example .env && python -m clickstream init-db
```

Generate a week of traffic, publish it, and build the warehouse:

```bash
python -m clickstream produce --sessions 2000 --days 7 --seed 42
```

```bash
python -m clickstream consume --idle-timeout 10
```

```bash
python -m clickstream transform && python -m clickstream test
```

Then look at the results:

```bash
python -m clickstream funnel
```

```
Conversion funnel -- (all channels)  (2,000 sessions entered)
  step              sessions  of prev  of all    lost
  page_view            2,000       -% 100.00%       0  ######################
  product_view         1,303   65.15%  65.15%     697  ##############........
  add_to_cart            714   54.80%  35.70%     589  ########..............
  checkout_start         484   67.79%  24.20%     230  #####.................
  purchase               391   80.79%  19.55%      93  ####..................
  revenue          $  130,930.10
```

---

## The three layers

### 1. Ingestion — Kafka

`clickstream/producer.py` publishes validated JSON events;
`clickstream/consumer.py` loads them into `raw.events`.

**Events are keyed by `anonymous_id`.** That is the load-bearing decision in
the whole ingestion design: one visitor's events always land on one partition
and therefore stay in order, which is what makes gap-based sessionization
downstream correct rather than approximate.

**Delivery is at-least-once from Kafka and effectively-once in the
warehouse.** The consumer:

1. buffers messages into a batch,
2. writes the rows *and* the batch record in **one** Postgres transaction,
3. commits Kafka offsets only after that transaction commits.

A crash between (2) and (3) replays the batch; every replayed row collides
with `raw.events`' primary key on `event_id` and is dropped by `ON CONFLICT
DO NOTHING`. A crash before (2) loses nothing, because offsets never moved.
Committing offsets for rows that failed to land is not reachable.

Verified rather than asserted — replaying the entire topic with a fresh
consumer group:

```
Read 13082 message(s) in 27 batch(es) -- stopped: idle
  inserted   0
  duplicate  13082  (already in raw.events)
  rejected   0
```

**Malformed messages are dead-lettered, not fatal.** Anything that fails
parsing or validation goes to `raw.events_dead_letter` with the reason, and
the consumer keeps going — a single bad producer cannot stall a partition.

### 2. Warehouse — a small dbt-style transform layer

`clickstream/transform/` is a miniature dbt: SQL models declare themselves,
reference each other, and the runner derives build order from that.

```sql
{{ config(materialized='incremental', unique_key='event_id') }}

select ... from {{ ref('stg_events') }}
```

| Supported | Meaning |
| --- | --- |
| `{{ config(...) }}` | `materialized`, `unique_key`, `schema`, `indexes`, `description` |
| `{{ ref('model') }}` | resolves to `schema.model` **and records a DAG edge** |
| `{{ source('raw','events') }}` | a table this project does not build |
| `{{ this }}` | the model's own relation |
| `{{ var('name') }}` | value injected by the runner |
| `{% if is_incremental() %}…{% endif %}` | kept only on incremental builds |

Materializations:

- **`view`** — dropped and recreated.
- **`table`** — built under a scratch name and swapped in by `ALTER TABLE …
  RENAME`, so the rebuild is atomic and readers never see it half-full.
- **`incremental`** — delete-and-insert merge on `unique_key`. Chosen over a
  plain append because staging re-reads a short overlap window on every run,
  so the same event can legitimately be selected twice and must update rather
  than duplicate.

Build order is resolved with Kahn's algorithm, ties broken alphabetically so
a run never reorders itself. `--select` pulls in a model's ancestors
automatically — running a model against stale upstreams is worse than not
running it.

### 3. Analytics — sessions and funnels

**Sessions are derived server-side, not sent by the client.** Events carry no
session id at all. `int_sessionized_events` cuts a new session whenever a
visitor's gap since their previous event exceeds `SESSION_TIMEOUT_MINUTES`
(default 30, the GA4 convention).

Client session ids drift — cookie loss, multiple tabs, clock skew — and once
written they cannot be re-cut. Deriving them means changing the timeout and
running `transform --full-refresh` re-sessionizes all of history.

Sessionization is a **full rebuild by design**. It is not incremental at the
tail: an event arriving now can extend a session whose rows were written on a
previous run, and only a full pass sees that.

**The funnel is sequential, not a set of independent event counts.**
`fct_funnel_steps` credits a session with step *N* only if it also reached
steps 1..*N*-1, each no later than the next. Counting `add_to_cart` events
directly would credit a session that deep-linked into a product page from an
email and skipped the steps before it — and the resulting "conversion rate"
can exceed 100%. A schema test asserts it cannot.

`agg_funnel_conversion` reports two different rates on purpose:

- `step_conversion_pct` — of sessions that reached the **previous** step, how
  many reached this one? *(where the leak is)*
- `overall_conversion_pct` — of **all** sessions that entered, how many got
  this far? *(how big the leak is)*

---

## Model DAG

```
raw.events  (source)
    │
    └─▶ stg_events                    incremental, merged on event_id
            │
            ├─▶ dim_visitor           one row per anonymous_id
            ├─▶ dim_page              one row per page path
            ├─▶ dim_product           one row per product
            │
            └─▶ int_sessionized_events   event → session bridge
                    │
                    ├─▶ fct_page_views      dwell time, entry/exit flags
                    │
                    └─▶ fct_sessions        ◀── the central fact
                            │
                            ├─▶ agg_daily_traffic     date × channel × device
                            │
                            └─▶ fct_funnel_steps      session × step reached
                                    │
                                    └─▶ agg_funnel_conversion
```

| Model | Grain |
| --- | --- |
| `stg_events` | one validated event |
| `int_sessionized_events` | one event → its session |
| `dim_visitor` | one `anonymous_id` (a device, not a person) |
| `dim_page` | one page path |
| `dim_product` | one product |
| `fct_page_views` | one `page_view` event |
| `fct_sessions` | one session |
| `fct_funnel_steps` | one (session, funnel step reached) |
| `agg_daily_traffic` | one (date, channel, device) |
| `agg_funnel_conversion` | one (channel, step) + all-channels rollup |

`stg` holds staging and intermediate models; only `mart` is meant to be
queried from outside the project.

---

## Commands

| Command | What it does |
| --- | --- |
| `init-db` | create/refresh the `raw` and `meta` schemas |
| `produce` | generate synthetic events and publish them |
| `consume` | load the topic into `raw.events` |
| `transform` | build the model DAG |
| `test` | run the schema tests |
| `pipeline` | consume → transform → test in one pass |
| `funnel` | conversion funnel, overall or `--by-channel` |
| `traffic` | daily traffic and channel mix |
| `pages` / `products` | page and product engagement |
| `status` | ingestion, batches, table counts, last run, failing tests |

Useful flags:

```bash
python -m clickstream produce --sessions 5000 --days 14 --rate 200
```

`--rate` paces publishing to simulate live traffic instead of a bulk dump.
`--dry-run` generates and summarises without touching Kafka; `--out FILE`
also writes JSON-lines.

```bash
python -m clickstream transform --select fct_sessions --full-refresh
```

Exit codes follow the batch convention: `0` ok, `1` partial, `2` failed.

> **Note on `--select`:** rebuilding a table drops it `CASCADE`, which also
> drops any dependent *view*. A full run recreates them in order; a partial
> run may not. After changing a model's shape, prefer a full `transform`.

---

## Data quality

Two independent test suites, and they check different things.

**`pytest` (110 tests)** — pure logic, no services needed:

```bash
python -m pytest
```

Covers event validation, timestamp normalisation, catalog parsing, model
config parsing, DAG resolution and cycle detection, SQL compilation, consumer
offset arithmetic, and the generator's statistical invariants.

**`python -m clickstream test` (101 tests)** — assertions against the built
warehouse, declared in `models/**/schema.yml`:

| Test | Scope |
| --- | --- |
| `not_null`, `unique` | column |
| `accepted_values` | column |
| `relationships` | column → referenced model |
| `assert_expression` | model (a boolean every row must satisfy) |
| `unique_combination` | model (a compound grain is unique) |

Results are recorded in `meta.test_results` and surfaced by `status`. The
sharpest ones are the funnel invariants — `sessions_reached <=
sessions_entered`, `overall_conversion_pct between 0 and 100` — because they
are exactly what breaks when a funnel is miscounted.

### Two properties worth calling out

Neither is asserted in prose only; both are checked by running the thing.

**Sessionization is independently correct.** The generator produced 2,000
sessions; the SQL sessionizer, which knows nothing about the generator, cut
`fct_sessions` to exactly 2,000 rows. A test in `test_generator.py`
reimplements the gap rule in Python and confirms it recovers the requested
count.

**Incremental loading does not duplicate.** After a second batch of 887
events, `stg_events` went from 13,082 to exactly 13,969 rows, with
`count(*) = count(distinct event_id)`.

---

## Configuration

All settings live in `.env` (copy from `.env.example`). The ones that change
behaviour rather than just addresses:

| Variable | Default | Effect |
| --- | --- | --- |
| `SESSION_TIMEOUT_MINUTES` | `30` | inactivity gap that closes a session |
| `KAFKA_TOPIC_PARTITIONS` | `6` | caps consumer parallelism |
| `CONSUMER_BATCH_SIZE` | `500` | rows per transaction |
| `CONSUMER_IDLE_TIMEOUT_SECONDS` | `0` | `0` = run until Ctrl-C |
| `GENERATOR_SEED` | *(unset)* | set for a reproducible stream |

Ports are chosen to stay out of the way of anything already running:
Postgres on **5435**, Kafka on **9094**.

---

## Layout

```
clickstream/
  config.py        settings, catalog, FUNNEL_STEPS (single source of truth)
  events.py        the event contract and its validation
  generator.py     synthetic traffic with a realistic funnel
  producer.py      topic provisioning and publishing
  consumer.py      batched, transactional, idempotent loading
  report.py        read-only queries behind the CLI
  transform/
    models.py      model parsing, DAG resolution, SQL compilation
    runner.py      materializations and execution
    tests.py       schema tests
models/
  staging/         stg_events + schema.yml
  intermediate/    int_sessionized_events + schema.yml
  marts/           dims, facts, aggregates + schema.yml
sql/001_schema.sql raw + meta (the model runner owns stg and mart)
tests/             pytest suite
```

`meta` carries the run log throughout: `ingest_batches`, `model_runs`,
`test_results`. Every batch, model build and test outcome is queryable after
the fact rather than living only in stdout.

---

## Known limitations

- **Single-node Kafka, replication factor 1.** Fine locally; `acks=all` and
  idempotent producing are configured correctly for a real cluster, but the
  broker itself is not durable.
- **`traffic` sums visitors across device rows**, so a person who used both a
  phone and a laptop in one day counts twice in that column. The exact figure
  needs a `count(distinct)` over `fct_sessions`; the aggregate keeps the cheap
  approximation and the code says so.
- **Session ids shift if events are backfilled *before* an existing session.**
  The id hashes (visitor, session start), so a late event that moves a
  session's start also changes its id. Backfilling *after* a session is safe.
- **No identity resolution.** `dim_visitor` is one row per `anonymous_id` — a
  device, not a person. `user_id` is carried but not used to stitch visitors
  across devices.
