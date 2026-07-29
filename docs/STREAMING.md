# Streaming lane — Spark Structured Streaming (M0–M2)

The batch lanes run on a schedule over closed periods. This lane runs **continuously
over a live market feed**, so the lakehouse tells a **batch + streaming (Lambda/Kappa)**
story rather than a batch-only one. Same storage format (Delta), same lineage columns,
same medallion layers — a different clock.

```
 Binance public WS         ofl stream-produce         ofl stream-bronze        ofl stream-silver
 <symbol>@trade   ─────► data/streaming/_landing/ ────►  bronze Delta   ─────►   silver Delta
 (free, no auth)          trades-*.jsonl                 trades/                 fact_trade_ohlc_1m/
                                                         trades_dead_letter/
                                                   ▲                       ▲
                                     _checkpoints/bronze_trades/   _checkpoints/silver_ohlc_1m/
                                     (offsets + commits)           (offsets + commits + window state)
```

**Implemented here: M0 (producer), M1 (bronze stream) and M2 (event-time silver).**
`Trigger.AvailableNow` and the R2/cron "live at R$0" tier are M3+ — see
[Roadmap](#roadmap) and the full spec in [`spec-streaming.md`](spec-streaming.md).

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

# M2 — stream bronze Delta into 1-minute event-time OHLC bars
uv run ofl stream-silver --seconds 120 --window "1 minute" --watermark "2 minutes"
```

All three commands are **capped by design**. The producer stops on `--max-seconds`,
`--max-events` or Ctrl-C, flushing its buffer on the way out; the Spark jobs stop on
`--seconds`. Nothing in this lane is meant to be left running unattended, and no
command needs another to be alive — the landing directory decouples M0 from M1, and
the bronze table decouples M1 from M2.

| Setting | Default | Meaning |
|---|---|---|
| `OFL_STREAMING_ROOT` | `data/streaming` | root for landing, bronze, silver and checkpoints |
| `OFL_STREAM_MAX_FILES_PER_TRIGGER` | `64` | backpressure: files a micro-batch may claim |

Everything under `data/streaming/` is generated and gitignored.

## Layout

```
data/streaming/
  _landing/                    producer output, one JSONL file per flush
  _landing_tmp/                partial writes; renamed into _landing atomically
  _checkpoints/bronze_trades/  offsets + commits — the exactly-once state
  _checkpoints/silver_ohlc_1m/ the silver query's own offsets, commits and window state
  bronze/trades/               bronze Delta: well-formed events
  bronze/trades_dead_letter/   bronze Delta: rejects, kept verbatim for replay
  silver/fact_trade_ohlc_1m/   silver Delta: 1-minute event-time OHLC bars
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

## M2 — event time, windows and the watermark

```
bronze/trades (Delta source)
  → withWatermark("trade_time", "2 minutes")      event time = the exchange's clock
  → dropDuplicatesWithinWatermark(["event_key"])  a sum is not idempotent
  → groupBy(window(trade_time, "1 minute"), symbol).agg(OHLC, volume, vwap)
  → silver/fact_trade_ohlc_1m (Delta, append)     one row per (window, symbol)
```

**Bronze is the source, not the landing directory.** Re-parsing JSON here would
duplicate M1's contract and its dead-letter routing. Delta is a first-class
streaming source, so silver reads the *table*: one parse, one dead-letter policy,
and silver inherits bronze's exactly-once guarantee instead of re-deriving it.

**Event time, not processing time.** `trade_time` is when the trade matched on the
exchange. A record that reaches us late is still assigned to the minute it *happened*
in, so the bars are reproducible: replay the stream and you get identical bars, which
is not true of a processing-time window. The [M1 reconciliation](#evidence--a-real-run)
already showed why this matters — a hand-injected event with a 23:56 timestamp
arriving at 00:14 must land in the 23:56 bar, and it does.

**Its own checkpoint.** A checkpoint belongs to one query: it holds that query's
source offsets *and* its operator state. Sharing bronze's would make each query
resume at the other's offset, and the aggregation state has nowhere to live there at
all. `_checkpoints/silver_ohlc_1m/` is therefore a sibling of `bronze_trades/`, not a
subdirectory — deleting it replays silver from bronze without touching bronze.

**Dedup before aggregation.** `dropDuplicatesWithinWatermark(["event_key"])` runs
*before* the `groupBy`, because `sum` is not idempotent: one duplicated trade
silently inflates a bar's volume forever. Unlike plain `dropDuplicates`, this variant
bounds its state by the watermark instead of remembering every key it has ever seen.

**Deterministic OHLC.** `open`/`close` are `min_by`/`max_by` over the composite
`(trade_time, trade_id)`, **not** `first`/`last`. A streaming aggregate has no defined
input order: `first()` returns whichever row the shuffle happened to hand over first
and can disagree with itself between runs. Ordering by `(trade_time, trade_id)` breaks
ties inside a millisecond the way the exchange itself does. The evidence that this
works is [in the bars below](#the-table): consecutive bars chain to within a single
tick, which is what continuous trading produces and what an arbitrary `first()` would
not.

**Append output mode.** A window is emitted exactly once, when the watermark passes
its end, so a plain Delta append is the correct sink — no MERGE. `foreachBatch` is
at-least-once on its own, so each write still carries `txnAppId`/`txnVersion`, exactly
as bronze does.

### The late-data policy

The watermark is a **promise to Spark**, not a filter: *"I will not ask you to
remember a window that ended more than 2 minutes before the largest event time I
have shown you."* That promise is what bounds state. The price is that anything
arriving after the promise expires is **dropped, silently, by design**.

| | rule |
|---|---|
| watermark | `max(trade_time) seen so far − 2 minutes`, and it only moves **forward** |
| a record is late | its `trade_time` is **strictly before** the watermark → dropped by the first stateful operator, never reaches the bar |
| a bar is published | when the watermark reaches its **end** (`watermark >= window_end`) — until then it sits in state and can still absorb late trades |
| the watermark is per **query**, not per key | a trade on one symbol advances the watermark for all of them |
| within a micro-batch | Spark uses the watermark computed from the **previous** batch, so the newest bars are never emitted in the same batch that fed them |

Why two minutes: it is two full bars. Long enough to absorb an ordinary
producer/consumer hiccup, short enough that state stays bounded and a bar is
published while it is still worth publishing. The trade is linear — a 10-minute
watermark would hold eight more bars per symbol in memory for the same data.

Nothing here is asserted from the manual. The rules above are reimplemented in
[`ofl/streaming/windows.py`](../ofl/streaming/windows.py) as plain Python — the
epoch-floored half-open window, the strictly-before lateness test, the
`watermark >= window_end` publish test — so
[`tests/test_streaming_windows.py`](../tests/test_streaming_windows.py) can pin the
boundary cases (`00:12:00.000` belongs to the *later* bar; an event exactly *on* the
watermark survives) without needing a JVM. The job itself uses those functions to
report which bars it is still holding when it stops.

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

## M2 — the late-data policy in three runs

Same day, same session, run over the 8,290-row bronze table above. Three runs, each
resuming from the silver checkpoint, chosen so that the watermark's two behaviours —
**holding bars back** and **dropping records** — are both visible rather than asserted.

### Run 1 — the bulk of bronze

```json
{"bronze": "file:///.../data/streaming/bronze/trades", "silver": "file:///.../data/streaming/silver/fact_trade_ohlc_1m", "checkpoint": "data\\streaming\\_checkpoints\\silver_ohlc_1m", "window": "1 minute", "watermark": "2 minutes", "max_files_per_trigger": 64, "trigger": "10 seconds", "event": "silver_stream_started"}
{"batch_id": 0, "windows": 0, "event": "micro_batch"}
{"batch_id": 1, "windows": 4, "event": "micro_batch"}
{"batchId": 2, "numInputRows": 0, "durationMs": 6, "watermark": "2026-07-29T00:12:20.516Z", "maxEventTime": null, "stateRows": 8298, "droppedByWatermark": 0, "event": "last_progress"}
{"batches": 2, "windows": 4, "dropped_late": 0, "watermark": "2026-07-29T00:12:20.516Z", "run_max_event_time": "2026-07-29T00:14:20.516Z", "state_rows": 8298, "open_windows": ["00:12:00", "00:13:00", "00:14:00"], "event": "silver_stream_stopped"}
```

**Batch 0 consumed all 8,290 rows and emitted nothing.** That is correct, not a bug:
a batch is evaluated against the watermark from the batch *before* it, and before
batch 0 there is no watermark. Batch 1 ran with the watermark now at
`00:12:20.516` — `00:14:20.516` minus two minutes — and published the 4 bars whose
end had been passed: `00:11` for each of the three symbols, plus the lone `23:56`
ETHUSDT bar from M1's hand-injected event, sitting 15 minutes behind the rest and
still landing in the minute it claims to have happened in.

`batchId 2` is a **no-data batch**: no new input, but Spark still runs a trigger so
the watermark can advance and state can be evicted. It found nothing more to close,
which is the second half of the story — the run stops with `00:12`, `00:13` and
`00:14` still open. Those bars are not lost, they are *unfinished*: the watermark has
not reached their end, so more trades could still legitimately arrive for them. A
bounded run over a finite source always leaves this tail in state.

`stateRows: 8298` = 8,290 dedup keys + 8 bars held open.

### Run 2 — three trades that arrive too late

Three BTCUSDT trades were injected into the landing directory with a `trade_time` of
`00:11:45` — behind the `00:12:20` watermark, and belonging to a bar that run 1 has
already published — at a deliberately absurd price so that any contamination would
be unmissable:

```json
{"e": "trade", "E": 1785283905000, "s": "BTCUSDT", "t": 6540602833, "p": "99999.00", "q": "1.00000000", "T": 1785283905000, "m": false, "M": true}
```

`ofl stream-bronze` accepted them without complaint — they are well-formed, so bronze
is the wrong layer to have an opinion about their timeliness:

```json
{"batch_id": 14, "bronze": 3, "dead_letter": 0, "event": "micro_batch"}
```

Silver disagrees:

```json
{"batch_id": 2, "windows": 0, "event": "micro_batch"}
{"batches": 1, "windows": 0, "dropped_late": 3, "watermark": "2026-07-29T00:12:20.516Z", "run_max_event_time": "2026-07-29T00:11:45.000Z", "state_rows": 8298, "open_windows": ["00:12:00", "00:13:00", "00:14:00"], "event": "silver_stream_stopped"}
```

**`dropped_late: 3`** — Spark's own `numRowsDroppedByWatermark`. The three rows were
discarded by `dropDuplicatesWithinWatermark` before reaching the aggregate; no bar was
written, no bar was modified, and the watermark did not move (it only ever goes
forward, and `00:11:45` is behind it). Note `run_max_event_time` is *older* than the
watermark: this is what a late-only run looks like, and why the job derives its
open-window report from the checkpointed watermark rather than from the events it
happened to see this run.

The 99999.00 price never appears in silver — that is the assertion this run exists to
make. A late record does not get a second chance, it gets counted.

### Run 3 — one on-time trade releases everything held

A single BTCUSDT trade at `00:17:00`, one symbol only:

```json
{"batch_id": 3, "windows": 0, "event": "micro_batch"}
{"batch_id": 4, "windows": 9, "event": "micro_batch"}
{"batchId": 5, "numInputRows": 0, "durationMs": 7, "watermark": "2026-07-29T00:15:00.000Z", "maxEventTime": null, "stateRows": 4505, "droppedByWatermark": 0, "event": "last_progress"}
{"batches": 2, "windows": 9, "dropped_late": 0, "watermark": "2026-07-29T00:15:00.000Z", "run_max_event_time": "2026-07-29T00:17:00.000Z", "state_rows": 4505, "open_windows": ["00:15:00", "00:16:00", "00:17:00"], "event": "silver_stream_stopped"}
```

That one trade pushed the watermark from `00:12:20` to `00:15:00` and released **9
bars** — `00:12`, `00:13` and `00:14` for all three symbols. One BTCUSDT trade closed
ETHUSDT and SOLUSDT bars, because **the watermark is a property of the query, not of
the key**. State fell from 8,298 rows to 4,505 as the evicted bars and the dedup keys
behind the new watermark were released. The tail moved rather than disappeared:
`00:15`, `00:16` and `00:17` are now the open ones.

### The table

```
silver rows: 13   delta version: 1   duplicate (window, symbol): 0

┌────────┬─────────┬──────────┬──────────┬──────────┬──────────┬───────────┬──────────┬────────┬────────────────────────┐
│ window ┆ symbol  ┆ open     ┆ high     ┆ low      ┆ close    ┆ volume    ┆ vwap     ┆ trades ┆ load_id                │
╞════════╪═════════╪══════════╪══════════╪══════════╪══════════╪═══════════╪══════════╪════════╪════════════════════════╡
│ 23:56  ┆ ETHUSDT ┆  1920.00 ┆  1920.00 ┆  1920.00 ┆  1920.00 ┆    0.5000 ┆  1920.00 ┆      1 ┆ ofl-streaming-silver:1 │
│ 00:11  ┆ BTCUSDT ┆ 64023.75 ┆ 64023.75 ┆ 64013.38 ┆ 64013.39 ┆    1.5794 ┆ 64021.42 ┆    343 ┆ ofl-streaming-silver:1 │
│ 00:11  ┆ ETHUSDT ┆  1922.69 ┆  1922.70 ┆  1922.35 ┆  1922.46 ┆   61.5562 ┆  1922.44 ┆    422 ┆ ofl-streaming-silver:1 │
│ 00:11  ┆ SOLUSDT ┆    73.99 ┆    74.00 ┆    73.99 ┆    73.99 ┆  121.1200 ┆    74.00 ┆     14 ┆ ofl-streaming-silver:1 │
│ 00:12  ┆ BTCUSDT ┆ 64013.39 ┆ 64042.00 ┆ 64013.39 ┆ 64041.99 ┆    2.5874 ┆ 64024.55 ┆   1332 ┆ ofl-streaming-silver:4 │
│ 00:12  ┆ ETHUSDT ┆  1922.46 ┆  1923.03 ┆  1922.25 ┆  1922.71 ┆  129.6237 ┆  1922.53 ┆   1477 ┆ ofl-streaming-silver:4 │
│ 00:12  ┆ SOLUSDT ┆    73.99 ┆    74.03 ┆    73.99 ┆    74.03 ┆ 1230.0320 ┆    74.00 ┆    198 ┆ ofl-streaming-silver:4 │
│ 00:13  ┆ BTCUSDT ┆ 64042.00 ┆ 64050.00 ┆ 64034.00 ┆ 64048.00 ┆    4.9715 ┆ 64046.06 ┆   1478 ┆ ofl-streaming-silver:4 │
│ 00:13  ┆ ETHUSDT ┆  1922.71 ┆  1924.63 ┆  1922.71 ┆  1924.16 ┆   86.9040 ┆  1923.61 ┆   1663 ┆ ofl-streaming-silver:4 │
│ 00:13  ┆ SOLUSDT ┆    74.04 ┆    74.06 ┆    74.02 ┆    74.06 ┆  555.2730 ┆    74.05 ┆    176 ┆ ofl-streaming-silver:4 │
│ 00:14  ┆ BTCUSDT ┆ 64048.01 ┆ 64048.01 ┆ 64034.01 ┆ 64034.01 ┆    0.5895 ┆ 64043.93 ┆    540 ┆ ofl-streaming-silver:4 │
│ 00:14  ┆ ETHUSDT ┆  1924.16 ┆  1924.32 ┆  1923.32 ┆  1923.32 ┆   45.2480 ┆  1924.17 ┆    613 ┆ ofl-streaming-silver:4 │
│ 00:14  ┆ SOLUSDT ┆    74.07 ┆    74.08 ┆    74.06 ┆    74.07 ┆   37.9570 ┆    74.08 ┆     33 ┆ ofl-streaming-silver:4 │
└────────┴─────────┴──────────┴──────────┴──────────┴──────────┴───────────┴──────────┴────────┴────────────────────────┘
```

(The `23:56` bar is the previous day — M1's hand-injected event, 15 minutes adrift of
everything else and still filed under the minute it claims.)

**Reconciliation.** Bronze now holds 8,294 rows (8,290 + 3 late + 1 on-time). The
`trades` column sums to **8,290** — exactly the original table:

| | rows | where they went |
|---|---:|---|
| bronze | 8,294 | |
| in the 13 published bars | 8,290 | `sum(trades)` |
| dropped as late | 3 | `dropped_late`, never reached a bar |
| still in state | 1 | the `00:17` BTCUSDT bar, watermark not there yet |

Two more things the table shows without being told.

**The bars chain.** Across the nine consecutive same-symbol pairs, a bar's `open` is
either exactly the previous bar's `close` (five pairs — every ETHUSDT pair, and
BTCUSDT `00:11`→`00:12` at `64013.39`) or one tick away from it (four pairs, all
`0.01`). That is what continuous trading looks like: the last trade of one minute and
the first of the next are *different trades*, so a one-tick gap is expected and a
larger one would not be. It is also the check that `min_by`/`max_by` are doing their
job — a non-deterministic `first()` would return an arbitrary price from anywhere
inside the minute, and the chain would break by far more than a tick.

**`load_id` records which run published each bar** — `:1` for run 1's four, `:4` for
run 3's nine. Bars are written once and never rewritten, which is what append mode
buys and why the sink needs no MERGE.

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

## Silver schema — `fact_trade_ohlc_1m`

Grain: **one row per (`window_start`, `symbol`)**.

| column | type | note |
|---|---|---|
| `window_start`, `window_end` | timestamp | half-open `[start, end)`, floored on the epoch |
| `symbol` | string | the other half of the grain |
| `open`, `close` | double | `min_by`/`max_by` over `(trade_time, trade_id)` — order-independent |
| `high`, `low` | double | plain `max`/`min`, which need no ordering |
| `volume` | double | `sum(quantity)` — base asset |
| `notional` | double | `sum(price × quantity)` — quote asset |
| `vwap` | double | `notional / volume`. The measure a bar exists to carry that `avg(price)` gets wrong, because it ignores trade size |
| `trades` | long | how many events built the bar; the reconciliation column |
| `sell_volume` | double | quantity where `buyer_is_maker` — the share that hit the bid |
| `first_trade_time`, `last_trade_time` | timestamp | observed event-time span *inside* the window, which is not the window's own span |
| `source`, `ingested_at`, `load_id` | | same lineage columns as bronze; `load_id` is `ofl-streaming-silver:<batch_id>` |

## Where it plugs into the medallion layers

| Layer | Batch lane | Streaming lane |
|---|---|---|
| bronze | Polars → one Delta table per series (`bronze/{fact}/{series}`) | Spark Structured Streaming → `bronze/trades` **+ dead letter** |
| silver | Spark idempotent `MERGE` → conformed star schema | watermarked event-time windows → `silver/fact_trade_ohlc_1m` |
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
| **M2** event-time silver | **done** | tumbling 1-min OHLC/volume with a watermark; `dropDuplicatesWithinWatermark` on `event_key`; own checkpoint |
| M3 `Trigger.AvailableNow` | next | same code, processes the delta since the checkpoint and exits — proves idempotency by running twice |
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
- Tests for the parts that don't need a JVM live in
  [`tests/test_streaming.py`](../tests/test_streaming.py) (wire contract, dedup key,
  atomic flush, frame unwrapping) and
  [`tests/test_streaming_windows.py`](../tests/test_streaming_windows.py) (window
  boundary math, watermark and late-event handling).
