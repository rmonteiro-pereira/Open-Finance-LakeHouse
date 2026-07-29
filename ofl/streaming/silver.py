"""M2 — event-time silver: watermarked 1-minute OHLC windows over bronze Delta.

    bronze/trades (Delta, append-only)          <- streaming source
      -> withWatermark("trade_time", delay)     <- event time, the exchange's clock
      -> dropDuplicatesWithinWatermark(event_key)
      -> groupBy(window(trade_time, "1 minute"), symbol).agg(OHLC, volume)
      -> silver/fact_trade_ohlc_1m (Delta, append)   <- its own checkpoint

Design notes worth the reading time:

* **Bronze is the source, not the landing directory.** Re-parsing JSON here would
  duplicate M1's contract and its dead-letter routing. Delta is a first-class
  streaming source, so silver reads the *table*: one parse, one dead-letter policy,
  and silver inherits bronze's exactly-once guarantee instead of re-deriving it.
* **Event time, not processing time.** ``trade_time`` is the exchange's clock. A
  record that reaches us late is still assigned to the minute it *happened* in, so
  the OHLC bars are stable and reproducible — replay the stream and you get the
  same bars, which is not true of a processing-time window.
* **The watermark is the contract about lateness.** ``withWatermark`` promises
  Spark it may forget any window that ended more than ``delay`` before the largest
  event time seen. That promise is what bounds state; the cost is that anything
  arriving after the promise is **dropped, silently, by design**. Spark counts
  those drops in ``numRowsDroppedByWatermark`` and this module surfaces the count
  in its run summary, because a late-data policy you cannot measure is a guess.
* **Dedup before aggregation.** ``dropDuplicatesWithinWatermark`` keys on
  ``event_key`` (``SYMBOL:trade_id``). Bronze is append-only and a replayed landing
  file could in principle re-present a trade; a sum is not idempotent, so the
  duplicate has to die *before* it reaches the aggregate. Unlike plain
  ``dropDuplicates``, this variant bounds its state by the watermark rather than
  keeping every key it has ever seen forever.
* **Append output mode.** A window is emitted exactly once, when the watermark
  passes its end — so a plain Delta append is the correct sink, no MERGE needed.
  ``foreachBatch`` is at-least-once on its own, so each write still carries Delta's
  ``txnAppId``/``txnVersion`` guard, exactly as bronze does.
* **Its own checkpoint.** A checkpoint holds one query's source offsets *and* its
  operator state. Sharing bronze's would make each query resume at the other's
  position; the aggregation state has nowhere to live in bronze's checkpoint at all.

Deterministic OHLC: ``open``/``close`` are ``min_by``/``max_by`` over
``(trade_time, trade_id)``, not ``first``/``last``. A streaming aggregate has no
defined input order — ``first()`` would return whichever row the shuffle happened
to hand over first, and would disagree with itself between runs. Ordering by the
composite key breaks ties within the same millisecond the way the exchange does,
by trade id.

Usage::

    ofl stream-silver --seconds 120 --window "1 minute" --watermark "2 minutes"
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from ofl.platform.io import to_local_uri
from ofl.platform.logging import get_logger
from ofl.streaming.bronze import trigger_for
from ofl.streaming.paths import (
    bronze_trades_dir,
    silver_checkpoint_dir,
    silver_ohlc_dir,
)
from ofl.streaming.schema import SOURCE
from ofl.streaming.windows import open_windows_at, parse_duration

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

#: Delta idempotency identity for this query. Distinct from bronze's: the two
#: queries commit independent batch-id sequences to different tables.
APP_ID = "ofl-streaming-silver"

QUERY_NAME = "silver_trades_ohlc_1m"

#: Default window width — the deliverable's 1-minute bar.
DEFAULT_WINDOW = "1 minute"

#: Default allowed lateness. Two minutes is two full bars: long enough that a
#: normal producer/consumer hiccup is absorbed, short enough that state stays
#: bounded and a bar is published while it is still worth publishing.
DEFAULT_WATERMARK = "2 minutes"

#: Event-time column. Bronze carries both ``trade_time`` (when the trade matched)
#: and ``event_time`` (when the exchange emitted it); the bar is about the trade.
EVENT_TIME = "trade_time"


def build_ohlc(
    events: "DataFrame", *, window: str = DEFAULT_WINDOW, watermark: str = DEFAULT_WATERMARK
) -> "DataFrame":
    """Watermark → dedup → tumbling-window OHLC. Pure DataFrame algebra, no I/O.

    Kept separate from the query wiring so the same expression can be applied to a
    static DataFrame (which is how it was first checked against bronze) and to a
    stream.
    """
    from pyspark.sql import functions as F

    deduped = events.withWatermark(EVENT_TIME, watermark).dropDuplicatesWithinWatermark(
        ["event_key"]
    )

    return (
        deduped.groupBy(F.window(F.col(EVENT_TIME), window), F.col("symbol"))
        .agg(
            # Deterministic under an undefined input order — see the module docstring.
            F.expr(f"min_by(price, struct({EVENT_TIME}, trade_id))").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.expr(f"max_by(price, struct({EVENT_TIME}, trade_id))").alias("close"),
            F.sum("quantity").alias("volume"),
            F.sum("notional").alias("notional"),
            F.count(F.lit(1)).alias("trades"),
            F.sum(F.when(F.col("buyer_is_maker"), F.col("quantity"))).alias("sell_volume"),
            F.min(EVENT_TIME).alias("first_trade_time"),
            F.max(EVENT_TIME).alias("last_trade_time"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("symbol"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume"),
            F.col("notional"),
            # Volume-weighted average price: the measure a bar exists to carry that
            # a naive avg(price) gets wrong, since it ignores trade size.
            (F.col("notional") / F.col("volume")).alias("vwap"),
            F.col("trades"),
            F.coalesce(F.col("sell_volume"), F.lit(0.0)).alias("sell_volume"),
            F.col("first_trade_time"),
            F.col("last_trade_time"),
        )
    )


def run_silver_stream(
    spark: "SparkSession",
    *,
    seconds: float | None = 120.0,
    trigger_interval: str = "10 seconds",
    available_now: bool = False,
    window: str = DEFAULT_WINDOW,
    watermark: str = DEFAULT_WATERMARK,
    max_files_per_trigger: int | None = None,
    app_id: str = APP_ID,
) -> dict:
    """Stream bronze Delta into the silver OHLC table until ``seconds`` elapse.

    Args:
        spark: session from :func:`ofl.streaming.bronze.build_streaming_session`.
        seconds: wall-clock cap on the run; ``None`` runs until interrupted (or,
            under ``available_now``, until the source is drained).
        trigger_interval: micro-batch cadence (processing-time trigger).
        available_now: use ``Trigger.AvailableNow`` instead — see :func:`trigger_for`.
        window: tumbling window width, e.g. ``"1 minute"``.
        watermark: allowed lateness on ``trade_time``.
        max_files_per_trigger: backpressure bound; defaults to the setting.
        app_id: Delta idempotency identity. Must be stable across restarts.
    """
    from ofl.config import get_settings

    window_delta = parse_duration(window)
    watermark_delta = parse_duration(watermark)
    if watermark_delta < window_delta:
        # Not fatal, but it means a bar can be closed before an event that belongs
        # one window back could plausibly arrive — say so rather than hide it.
        log.warning(
            "watermark_shorter_than_window",
            window=window,
            watermark=watermark,
            note="events one window late are always dropped",
        )

    bronze_path = to_local_uri(bronze_trades_dir())
    silver_path = to_local_uri(silver_ohlc_dir())
    checkpoint = silver_checkpoint_dir()
    checkpoint.mkdir(parents=True, exist_ok=True)
    max_files = max_files_per_trigger or get_settings().streaming_max_files_per_trigger

    bronze = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", max_files)
        .load(bronze_path)
    )
    bars = build_ohlc(bronze, window=window, watermark=watermark)

    stats = {"batches": 0, "windows": 0}

    def write_batch(batch: "DataFrame", batch_id: int) -> None:
        from pyspark.sql import functions as F

        load_id = f"{app_id}:{batch_id}"
        out = batch.select(
            "*",
            F.lit(SOURCE).alias("source"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(load_id).alias("load_id"),
        )
        out.persist()
        try:
            n = out.count()
            if n:
                (
                    out.write.format("delta")
                    .mode("append")
                    # Same idempotency guard as bronze: foreachBatch is at-least-once,
                    # the Delta log makes the replay of a committed batch a no-op.
                    .option("txnAppId", app_id)
                    .option("txnVersion", batch_id)
                    .save(silver_path)
                )
            stats["batches"] += 1
            stats["windows"] += n
            log.info("micro_batch", batch_id=batch_id, windows=n)
        finally:
            out.unpersist()

    trigger = trigger_for(available_now=available_now, interval=trigger_interval)
    query = (
        bars.writeStream.queryName(QUERY_NAME)
        .outputMode("append")
        .option("checkpointLocation", to_local_uri(checkpoint))
        .trigger(**trigger)
        .foreachBatch(write_batch)
        .start()
    )
    log.info(
        "silver_stream_started",
        bronze=bronze_path,
        silver=silver_path,
        checkpoint=str(checkpoint),
        window=window,
        watermark=watermark,
        max_files_per_trigger=max_files,
        trigger="availableNow" if available_now else trigger_interval,
    )

    try:
        if seconds:
            query.awaitTermination(timeout=seconds)
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        log.info("silver_stream_interrupted")
    finally:
        progress = [_progress_row(p) for p in query.recentProgress]
        query.stop()

    mode = "availableNow" if available_now else f"processingTime:{trigger_interval}"
    summary = {**stats, "mode": mode, "silver_uri": silver_path, "progress": progress}
    summary.update(_late_data_report(progress, window=window, watermark=watermark))
    if progress:
        log.info("last_progress", **progress[-1])
    log.info("silver_stream_stopped", **{k: v for k, v in summary.items() if k != "progress"})
    return summary


def _late_data_report(progress: list[dict], *, window: str, watermark: str) -> dict:
    """What the watermark actually did this run, from Spark's own metrics.

    ``dropped_late`` is the honest number: rows a stateful operator discarded
    because their event time was already behind the watermark. The still-open
    windows are the mirror image — bars held back because the watermark has *not*
    reached their end yet, and which a later run will emit.

    The open windows are derived from the **watermark**, not from the largest event
    time of this run: the watermark is what the checkpoint carries across restarts,
    and a run that ingests nothing but late data has a max event time far behind it.
    """
    dropped = sum(p.get("droppedByWatermark") or 0 for p in progress)
    last = progress[-1] if progress else {}
    mark = last.get("watermark")
    report: dict = {
        "dropped_late": dropped,
        "watermark": mark,
        "run_max_event_time": max(
            (p["maxEventTime"] for p in progress if p.get("maxEventTime")), default=None
        ),
        "state_rows": last.get("stateRows"),
    }
    if mark:
        still_open = open_windows_at(_parse_ts(mark), window=window, delay=watermark)
        report["open_windows"] = [w[0].strftime("%H:%M:%S") for w in still_open]
    return report


def _parse_ts(text: str) -> datetime:
    """Spark renders progress timestamps as ISO-8601 with a literal ``Z``."""
    return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)


def _progress_row(progress) -> dict:
    """The progress fields that make the watermark's behaviour legible."""
    p = progress if isinstance(progress, dict) else json.loads(progress.json)
    event_time = p.get("eventTime") or {}
    state = p.get("stateOperators") or []
    return {
        "batchId": p.get("batchId"),
        "numInputRows": p.get("numInputRows"),
        "durationMs": p.get("durationMs", {}).get("triggerExecution"),
        # The watermark Spark used for *this* batch — always derived from the
        # previous one, which is why the newest windows are never emitted at once.
        "watermark": event_time.get("watermark"),
        "maxEventTime": event_time.get("max"),
        "stateRows": sum(op.get("numRowsTotal", 0) for op in state),
        "droppedByWatermark": sum(op.get("numRowsDroppedByWatermark", 0) for op in state),
    }
