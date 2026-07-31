"""M1 — Spark Structured Streaming: landing JSONL → bronze Delta, with checkpoint.

``readStream`` (file source, explicit schema) → parse → split → ``writeStream``
(Delta, append) with an explicit checkpoint location.

Design notes worth the reading time:

* **Text source, not JSON source.** Reading the files as text and applying
  ``from_json`` ourselves means a malformed line becomes a *row we can route*
  rather than a task failure or a silently dropped record. That is what makes the
  dead-letter table possible.
* **One query, two sinks, via ``foreachBatch``.** Good records and rejects come
  from the same parse, so they must advance the same offsets — two independent
  ``writeStream``s would read the landing directory twice and keep two checkpoints
  that could disagree. Inside ``foreachBatch`` each Delta write carries
  ``txnAppId``/``txnVersion``, so a batch replayed after a mid-write crash is
  recognised by the Delta log and skipped: the pair stays exactly-once.
* **Case sensitivity.** The exchange distinguishes ``E``/``e`` and ``T``/``t``;
  Spark's default case-insensitive resolution cannot, so this session flips
  ``spark.sql.caseSensitive``.
* **Backpressure.** ``maxFilesPerTrigger`` bounds a micro-batch. It matters on
  restart, when the job faces a backlog instead of a live tail.

Usage::

    ofl stream-bronze --seconds 90
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from ofl.platform.io import to_local_uri
from ofl.platform.logging import get_logger
from ofl.streaming.paths import (
    bronze_trades_dir,
    checkpoint_dir,
    dead_letter_dir,
    landing_dir,
)
from ofl.streaming.schema import (
    DEDUP_KEY_SQL,
    REQUIRED_FIELDS,
    SOURCE,
    TRADE_EVENT_DDL,
    ddl_field_names,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

#: Stable across restarts — Delta keys its idempotency ledger on it, so changing
#: this string makes already-committed batches replayable (and thus duplicated).
APP_ID = "ofl-streaming-bronze"

QUERY_NAME = "bronze_trades"

STREAMING_CONF = {
    # The exchange's wire format uses case to separate distinct fields (`E` event
    # time vs `e` event type). Case-insensitive resolution would make `from_json`
    # ambiguous on them.
    "spark.sql.caseSensitive": "true",
    # Local, low-volume stream: a handful of files per batch does not deserve 200
    # shuffle partitions or many small output files per micro-batch.
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.streaming.metricsEnabled": "true",
    # This lane always runs in local mode (laptop, or an ephemeral CI runner in
    # M3+), so driver and executor are the same JVM. On a multi-homed host Spark
    # otherwise picks whatever interface it finds first — a VPN or virtual adapter
    # can hand it a link-local 169.254.x.x address, and the driver then times out
    # connecting to itself. Loopback is deterministic.
    "spark.driver.host": "127.0.0.1",
    "spark.driver.bindAddress": "127.0.0.1",
}


def build_streaming_session(app_name: str = APP_ID) -> "SparkSession":
    from ofl.platform.spark import build_spark_session

    spark = build_spark_session(app_name, extra_conf=STREAMING_CONF)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def trigger_for(*, available_now: bool, interval: str) -> dict:
    """The ``writeStream.trigger(**kwargs)`` for one of the lane's two modes.

    * **Processing-time** (``interval``) is the *continuous* mode: the query stays
      up and fires a micro-batch on a clock. It is how M0–M2 were developed, and
      what you want when watching a live feed.
    * **``Trigger.AvailableNow``** is the *cron* mode: the query claims everything
      the source has right now, processes it in as many micro-batches as
      ``maxFilesPerTrigger`` implies, and **exits on its own**. That is what makes a
      GitHub Actions run possible — a scheduled job must terminate.

    The same query, the same checkpoint, the same code path. Only the trigger
    differs, which is the point: the exactly-once story is carried by the
    checkpoint plus ``txnAppId``/``txnVersion``, not by the mode. Two consecutive
    ``AvailableNow`` runs over an unchanged source therefore have to leave the
    tables byte-for-byte equivalent — that is what ``tools/streaming_idempotence.py``
    measures rather than asserts.
    """
    return {"availableNow": True} if available_now else {"processingTime": interval}


def _prepare(raw: "DataFrame") -> "DataFrame":
    """Parse the raw text lines against the explicit schema and tag admissibility.

    Splitting is expressed as columns rather than as two filtered reads so that the
    classification travels with the row into ``foreachBatch``.
    """
    from pyspark.sql import functions as F

    valid = F.lit(True)
    for field in REQUIRED_FIELDS:
        valid = valid & F.col(f"evt.{field}").isNotNull()

    # `from_json` is PERMISSIVE: a line it cannot parse yields a struct of nulls
    # rather than a null struct, so "nothing came back at all" is the honest test
    # for unparseable input — otherwise garbage is mislabelled as merely incomplete.
    nothing_parsed = F.col("evt").isNull()
    all_null = F.lit(True)
    for field in ddl_field_names():
        all_null = all_null & F.col(f"evt.{field}").isNull()

    missing = F.concat_ws(
        ",", *[F.when(F.col(f"evt.{f}").isNull(), F.lit(f)) for f in REQUIRED_FIELDS]
    )
    reason = F.when(nothing_parsed | all_null, F.lit("unparseable_json")).otherwise(
        F.concat(F.lit("missing_required_fields:"), missing)
    )

    return (
        raw.withColumn("source_file", F.input_file_name())
        .withColumn("evt", F.from_json(F.col("value"), TRADE_EVENT_DDL))
        .withColumn("_valid", valid)
        .withColumn("_reason", reason)
        .withColumn("ingested_at", F.current_timestamp())
    )


def _project_bronze(batch: "DataFrame", load_id: str) -> "DataFrame":
    """Typed bronze projection: exchange strings become real types, grain is explicit."""
    from pyspark.sql import functions as F

    price = F.col("evt.p").cast("double")
    qty = F.col("evt.q").cast("double")
    return batch.select(
        F.expr(DEDUP_KEY_SQL).alias("event_key"),
        F.col("evt.s").alias("symbol"),
        F.col("evt.t").alias("trade_id"),
        price.alias("price"),
        qty.alias("quantity"),
        (price * qty).alias("notional"),
        F.col("evt.m").alias("buyer_is_maker"),
        # Epoch millis on the wire. `trade_time` is the event time M2 watermarks on;
        # `event_time` is when the exchange emitted it — keeping both is what makes
        # event-time vs processing-time skew measurable rather than assumed.
        (F.col("evt.T") / 1000).cast("timestamp").alias("trade_time"),
        (F.col("evt.E") / 1000).cast("timestamp").alias("event_time"),
        F.lit(SOURCE).alias("source"),
        F.col("source_file"),
        F.col("ingested_at"),
        F.lit(load_id).alias("load_id"),
    )


def _project_dead_letter(batch: "DataFrame", load_id: str) -> "DataFrame":
    """Rejects keep the original line verbatim — the point is to be able to replay it."""
    from pyspark.sql import functions as F

    return batch.select(
        F.col("value").alias("raw"),
        F.col("_reason").alias("reason"),
        F.lit(SOURCE).alias("source"),
        F.col("source_file"),
        F.col("ingested_at"),
        F.lit(load_id).alias("load_id"),
    )


def _write_delta(df: "DataFrame", path: str, *, batch_id: int, app_id: str) -> None:
    (
        df.write.format("delta")
        .mode("append")
        # Delta's idempotent-write protocol: the transaction log remembers the last
        # version committed for this app, so a replayed batch is a no-op.
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .save(path)
    )


def run_bronze_stream(
    spark: "SparkSession",
    *,
    seconds: float | None = 120.0,
    trigger_interval: str = "10 seconds",
    available_now: bool = False,
    max_files_per_trigger: int | None = None,
    app_id: str = APP_ID,
) -> dict:
    """Stream the landing directory into bronze Delta until ``seconds`` elapse.

    Args:
        spark: session from :func:`build_streaming_session`.
        seconds: wall-clock cap on the run; ``None`` runs until interrupted (or,
            under ``available_now``, until the landing directory is drained).
        trigger_interval: micro-batch cadence (processing-time trigger).
        available_now: use ``Trigger.AvailableNow`` instead — see :func:`trigger_for`.
        max_files_per_trigger: backpressure bound; defaults to the setting.
        app_id: Delta idempotency identity. Must be stable across restarts.
    """
    from ofl.config import get_settings

    landing = landing_dir()
    landing.mkdir(parents=True, exist_ok=True)  # the file source requires the dir to exist
    checkpoint = checkpoint_dir()
    checkpoint.mkdir(parents=True, exist_ok=True)

    bronze_path = to_local_uri(bronze_trades_dir())
    dead_path = to_local_uri(dead_letter_dir())
    max_files = max_files_per_trigger or get_settings().streaming_max_files_per_trigger

    raw = (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", max_files)
        .load(to_local_uri(landing))
    )
    prepared = _prepare(raw)

    stats = {"batches": 0, "bronze_rows": 0, "dead_rows": 0}

    def write_batch(batch: "DataFrame", batch_id: int) -> None:
        # Both sinks and both counts read the same batch; materialise it once.
        batch.persist()
        try:
            load_id = f"{app_id}:{batch_id}"
            good = _project_bronze(batch.filter("_valid"), load_id)
            bad = _project_dead_letter(batch.filter("NOT _valid"), load_id)
            n_good, n_bad = good.count(), bad.count()

            if n_good:
                _write_delta(good, bronze_path, batch_id=batch_id, app_id=app_id)
            if n_bad:
                _write_delta(bad, dead_path, batch_id=batch_id, app_id=f"{app_id}-dlq")

            stats["batches"] += 1
            stats["bronze_rows"] += n_good
            stats["dead_rows"] += n_bad
            log.info("micro_batch", batch_id=batch_id, bronze=n_good, dead_letter=n_bad)
        finally:
            batch.unpersist()

    trigger = trigger_for(available_now=available_now, interval=trigger_interval)
    query = (
        prepared.writeStream.queryName(QUERY_NAME)
        .option("checkpointLocation", to_local_uri(checkpoint))
        .trigger(**trigger)
        .foreachBatch(write_batch)
        .start()
    )
    log.info(
        "stream_started",
        landing=str(landing),
        bronze=bronze_path,
        checkpoint=str(checkpoint),
        max_files_per_trigger=max_files,
        trigger="availableNow" if available_now else trigger_interval,
    )

    try:
        if seconds:
            query.awaitTermination(timeout=seconds)
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        log.info("stream_interrupted")
    finally:
        progress = [_progress_row(p) for p in query.recentProgress]
        query.stop()

    summary = {**stats, "progress": progress, "bronze_uri": bronze_path}
    if progress:
        # Spark's own view of the last micro-batch — throughput and trigger cost as
        # measured by the engine, not by our counters.
        log.info("last_progress", **progress[-1])
    log.info("stream_stopped", **stats)
    return summary


def _progress_row(progress) -> dict:
    """The handful of progress fields worth keeping as evidence."""
    p = progress if isinstance(progress, dict) else json.loads(progress.json)
    return {
        "batchId": p.get("batchId"),
        "numInputRows": p.get("numInputRows"),
        "inputRowsPerSecond": p.get("inputRowsPerSecond"),
        "processedRowsPerSecond": p.get("processedRowsPerSecond"),
        "durationMs": p.get("durationMs", {}).get("triggerExecution"),
    }
