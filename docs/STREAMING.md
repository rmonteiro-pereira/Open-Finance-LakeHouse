# Streaming lane — Spark Structured Streaming (M0–M1)

The batch lanes run on a schedule over closed periods. This lane runs **continuously
over a live market feed**, so the lakehouse tells a **batch + streaming (Lambda/Kappa)**
story rather than a batch-only one. Same storage format (Delta), same lineage columns,
same medallion layers — a different clock.

```
 Binance public WS          ofl stream-produce            ofl stream-bronze
 <symbol>@trade   ─────►  data/streaming/_landing/  ─────►  bronze Delta  ──► (M2) silver
 (free, no auth)           trades-*.jsonl                   trades/
                                                            trades_dead_letter/
                                                    ▲
                                          _checkpoints/bronze_trades/
                                          (offsets + commits = exactly-once)
```

**Implemented here: M0 (producer) and M1 (bronze stream).** Event-time windows,
watermarks, `Trigger.AvailableNow` and the R2/cron "live at R$0" tier are M2+ —
see [Roadmap](#roadmap) and the full spec in [`spec-streaming.md`](spec-streaming.md).

## Why this source

Binance's public `@trade` WebSocket is free, needs no key and no account, and is
market data — it matches the lakehouse's financial theme instead of bolting on an
unrelated feed. Wikimedia EventStreams (SSE) is the documented fallback if the
exchange endpoint is ever unreachable.

## Running it

```bash
uv sync --extra spark --extra streaming     # pyspark + delta-spark + websockets

# M0 — capture live trades to the landing directory (always terminates)
uv run ofl stream-produce --symbols btcusdt,ethusdt,solusdt --max-seconds 170

# M1 — stream the landing directory into bronze Delta (own terminal, can run
# concurrently with the producer to watch it tail a live feed)
uv run ofl stream-bronze --seconds 120 --trigger "10 seconds"
```

Both commands are **capped by design**. The producer stops on `--max-seconds`,
`--max-events` or Ctrl-C, flushing its buffer on the way out; the Spark job stops on
`--seconds`. Nothing in this lane is meant to be left running unattended, and
neither command needs the other to be alive — the landing directory decouples them.

| Setting | Default | Meaning |
|---|---|---|
| `OFL_STREAMING_ROOT` | `data/streaming` | root for landing, bronze and checkpoints |
| `OFL_STREAM_MAX_FILES_PER_TRIGGER` | `64` | backpressure: files a micro-batch may claim |

Everything under `data/streaming/` is generated and gitignored.

## Layout

```
data/streaming/
  _landing/                    producer output, one JSONL file per flush
  _landing_tmp/                partial writes; renamed into _landing atomically
  _checkpoints/bronze_trades/  offsets + commits — the exactly-once state
  bronze/trades/               bronze Delta: well-formed events
  bronze/trades_dead_letter/   bronze Delta: rejects, kept verbatim for replay
```

`_landing_tmp` is a **sibling** of `_landing`, not a child. Spark's file source lists
a directory and reads whatever it finds, so a half-written file would be parsed as
truncated JSON. The producer builds each file in the tmp directory and then does a
single `os.replace` into the watched directory: the file appears complete or not at all.

## Explicit schema, and where bad records go

A stream is **never** schema-inferred. Inference samples whatever files happen to
exist at start-up, drifts silently when the feed changes, and doesn't survive a
restart from checkpoint. The wire format is pinned in
[`ofl/streaming/schema.py`](../ofl/streaming/schema.py) as a DDL string and handed to
`from_json`.

The job reads the landing files with the **text** source rather than the JSON source.
That is deliberate: with `from_json` applied by us, a malformed line becomes *a row we
can route* instead of a task failure or a silently dropped record. Records split
per-record, not per-file — a single file can contribute to both tables:

| condition | destination | `reason` |
|---|---|---|
| parses, and `s`/`t`/`p`/`q`/`T` are all present | `bronze/trades` | — |
| parses, but a required field is missing | `bronze/trades_dead_letter` | `missing_required_fields:t` |
| does not parse at all | `bronze/trades_dead_letter` | `unparseable_json` |

Rejects keep the original line verbatim, plus `source_file`, `ingested_at` and
`load_id` — the point of a dead letter is to be able to replay it once the cause is
understood.

> **Gotcha worth knowing:** the exchange uses *case* to distinguish fields — `E`
> (event time) vs `e` (event type), `T` (trade time) vs `t` (trade id). Spark resolves
> struct fields case-insensitively by default, which makes those pairs ambiguous, so
> the streaming session sets `spark.sql.caseSensitive=true`.

## Checkpointing and exactly-once

The checkpoint directory is what makes this lane restartable rather than merely
re-runnable. Two mechanisms combine:

1. **Source side — the checkpoint.** `_checkpoints/bronze_trades/offsets` records which
   landing files each batch claimed *before* the batch runs; `commits` records that it
   finished. On restart Spark resumes at the next batch id and never re-lists a file it
   has already committed. Deleting the checkpoint is how you deliberately replay.

2. **Sink side — idempotent Delta writes.** Good records and rejects come from the same
   parse, so they must advance the *same* offsets. Two independent `writeStream`s would
   read the landing directory twice and keep two checkpoints that could disagree, so the
   job uses a single query with `foreachBatch` and two Delta writes. `foreachBatch` gives
   at-least-once by itself, so each write carries Delta's idempotency options —
   `txnAppId` (stable across restarts) and `txnVersion` (the batch id). The Delta log
   remembers the last version committed for that app, so a batch replayed after a
   mid-write crash is recognised and skipped. The pair stays exactly-once.

`maxFilesPerTrigger` bounds a micro-batch. It rarely binds while tailing a live
producer; it matters on **restart**, when the job faces a backlog instead of a tail.

## Evidence — a real run

Captured 2026-07-29, local Spark 3.5.5 (`local[*]`), Delta 3.2, against the live
Binance feed. Full console output is reproducible with the commands above.

**M0 — producer** (3 symbols, 170s cap):

```json
{"url": "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade", "landing": "data\\streaming\\_landing", "max_seconds": 170.0, "max_events": 20000, "event": "producer_start"}
{"file": "trades-20260729T001141713984-00000.jsonl", "events": 11, "event": "landing_flush"}
{"file": "trades-20260729T001148406583-00001.jsonl", "events": 227, "event": "landing_flush"}
...
{"events": 8289, "skipped": 0, "files": 34, "landed": 8289, "seconds": 173.0, "event": "producer_done"}
```

**M1 — first run**, started while the producer was still writing, so it tails a live
feed rather than draining a static directory:

```json
{"landing": "data\\streaming\\_landing", "bronze": "file:///.../data/streaming/bronze/trades", "checkpoint": "data\\streaming\\_checkpoints\\bronze_trades", "max_files_per_trigger": 64, "trigger": "10 seconds", "event": "stream_started"}
{"batch_id": 0,  "bronze": 462,  "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 1,  "bronze": 14,   "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 2,  "bronze": 770,  "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 3,  "bronze": 558,  "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 4,  "bronze": 244,  "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 5,  "bronze": 155,  "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 6,  "bronze": 256,  "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 7,  "bronze": 1141, "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 8,  "bronze": 164,  "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 9,  "bronze": 455,  "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 10, "bronze": 915,  "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 11, "bronze": 1000, "dead_letter": 0, "event": "micro_batch"}
{"batch_id": 12, "bronze": 490,  "dead_letter": 0, "event": "micro_batch"}
{"batches": 13, "bronze_rows": 6624, "dead_rows": 0, "event": "stream_stopped"}
```

Batch 11's suspiciously round `1000` is not `maxFilesPerTrigger` biting — it is two
producer files that each hit the `flush_events` cap of 500 during a busy stretch.

**M1 — second run, after injecting four hand-written lines** (one truncated, one
missing its trade id, one not JSON at all, one valid) into a single landing file. Note
the batch id: the query resumes at **13**, from the checkpoint, not from 0.

```json
{"batch_id": 13, "bronze": 1666, "dead_letter": 3, "event": "micro_batch"}
{"batches": 1, "bronze_rows": 1666, "dead_rows": 3, "event": "stream_stopped"}
```

The dead-letter table, read back with `deltalake`:

```
┌───────────────────────────┬──────────────────────────────────────────────────────────────────────────────────┐
│ reason                    ┆ raw                                                                              │
╞═══════════════════════════╪══════════════════════════════════════════════════════════════════════════════════╡
│ unparseable_json          ┆ {"e":"trade","E":1785283000000,"s":"BTCUSDT","t":1,"p":"1.0"                     │
│ missing_required_fields:t ┆ {"e":"trade","E":1785283000001,"s":"BTCUSDT","p":"117000.00","q":"0.01",...}      │
│ unparseable_json          ┆ not json at all                                                                  │
└───────────────────────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

The fourth line of that same file — the valid one — landed in `bronze/trades` as
`ETHUSDT:2`. Routing is per record, not per file.

**M1 — third run, no new data.** The restart is a no-op, which is the whole point:

```json
{"batchId": 14, "numInputRows": 0, "inputRowsPerSecond": 0.0, "processedRowsPerSecond": 0.0, "durationMs": 13, "event": "last_progress"}
{"batches": 0, "bronze_rows": 0, "dead_rows": 0, "event": "stream_stopped"}
```

**Reconciliation.** The producer landed 8,289 events; one valid event was injected by
hand; bronze holds 8,290 rows with **8,290 distinct `event_key`s — zero duplicates**
across two stops and three starts:

```
bronze rows:             8290
delta version:           13          (one commit per micro-batch)
distinct event_key:      8290
duplicate event_keys:    0
distinct load_id:        14          (= ofl-streaming-bronze:0 .. :13)

┌─────────┬────────┬───────────┬───────────┬───────────────┬─────────────┬────────────┐
│ symbol  ┆ trades ┆ min_price ┆ max_price ┆ notional_usdt ┆ first_trade ┆ last_trade │
╞═════════╪════════╪═══════════╪═══════════╪═══════════════╪═════════════╪════════════╡
│ ETHUSDT ┆ 4176   ┆ 1920.0    ┆ 1924.63   ┆ 622738.0      ┆ 23:56:40    ┆ 00:14:20   │
│ BTCUSDT ┆ 3693   ┆ 64013.38  ┆ 64050.0   ┆ 622930.0      ┆ 00:11:32    ┆ 00:14:20   │
│ SOLUSDT ┆ 421    ┆ 73.99     ┆ 74.08     ┆ 143917.0      ┆ 00:11:32    ┆ 00:14:18   │
└─────────┴────────┴───────────┴───────────┴───────────────┴─────────────┴────────────┘
```

(`ETHUSDT`'s `first_trade` of 23:56:40 is the hand-injected event's fabricated
timestamp, not a real trade — visible proof that event time is the *exchange's* clock,
not ingestion time, which is exactly the skew M2's watermark has to handle.)

## Bronze schema

| column | type | note |
|---|---|---|
| `event_key` | string | `SYMBOL:trade_id` — the dedup/MERGE key |
| `symbol` | string | grain |
| `trade_id` | long | unique *within* a symbol only |
| `price`, `quantity`, `notional` | double | cast from the exchange's decimal strings |
| `buyer_is_maker` | boolean | trade hit the bid |
| `trade_time` | timestamp | **event time** — what M2 watermarks on |
| `event_time` | timestamp | when the exchange emitted it |
| `source` | string | `binance_ws` |
| `source_file` | string | lineage back to the landing file |
| `ingested_at` | timestamp | processing time |
| `load_id` | string | `ofl-streaming-bronze:<batch_id>` |

`source` / `ingested_at` / `load_id` mirror the batch lanes' lineage columns
(`ofl/ingestion/landing.py`), so the two lanes stay legible side by side.

## Where it plugs into the medallion layers

| Layer | Batch lane | Streaming lane |
|---|---|---|
| bronze | Polars → one Delta table per series (`bronze/{fact}/{series}`) | Spark Structured Streaming → `bronze/trades` **+ dead letter** |
| silver | Spark idempotent `MERGE` → conformed star schema | **M2:** watermarked event-time windows → `silver.fact_trade_ohlc_1m` |
| gold | DuckDB SQL marts | **M5:** a near-real-time mart beside the batch marts |

The join is at **silver**, not bronze: the streaming bronze table is deliberately raw
and append-only, exactly like the batch bronze tables. `event_key` exists precisely so
the silver MERGE has an idempotent match key, which is the same technique
`ofl/transform/spark/silver.py` already uses for `fact_observation` — the streaming
lane reuses the lakehouse's semantics rather than inventing parallel ones.

Storage today is local (`data/streaming/`). The production MinIO `lakehouse` bucket is
**untouched by this lane** — checkpoints are hot, high-churn state and belong next to
whichever compute owns them, which is why M3 repoints `OFL_STREAMING_ROOT` at object
storage rather than sharing the batch bucket.

## Roadmap

| Milestone | Status | What it adds |
|---|---|---|
| **M0** producer | **done** | live WS → append-only JSONL landing |
| **M1** bronze stream | **done** | `readStream` → `writeStream` Delta, checkpoint, explicit schema, dead letter |
| M2 event-time silver | next | tumbling 1-min OHLC/volume with a watermark; `dropDuplicatesWithinWatermark` on `event_key` |
| M3 `Trigger.AvailableNow` | | same code, processes the delta since the checkpoint and exits — proves idempotency by running twice |
| M4 zero-cost live | | GitHub Actions cron + Cloudflare R2 for Delta/checkpoint; metrics snapshot → dashboard |
| M5 integration | | silver/gold consume the streaming table; batch + streaming writeup and diagram |

The order matters: **local continuous first** to get the semantics right, then
`AvailableNow` + cron for the cost story. The checkpoint is what guarantees
exactly-once in both modes — that is why it is the piece M1 builds properly.

## Notes

- **Local mode only.** The lane pins `spark.driver.host`/`bindAddress` to `127.0.0.1`.
  On a multi-homed host Spark otherwise picks whatever interface it finds first; a VPN
  or virtual adapter can hand it a link-local `169.254.x.x` address and the driver then
  times out connecting to itself.
- **On Windows**, Spark needs `HADOOP_HOME` pointing at a directory containing
  `bin/winutils.exe` and `bin/hadoop.dll` to write local files.
- Tests for the parts that don't need a JVM (wire contract, dedup key, atomic flush,
  frame unwrapping) live in [`tests/test_streaming.py`](../tests/test_streaming.py).
