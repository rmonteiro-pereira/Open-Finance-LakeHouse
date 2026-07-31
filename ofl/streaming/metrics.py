"""M4 — per-run metrics snapshot for the streaming lane.

Two halves, deliberately separate:

* **Run metrics** come from Spark's own ``StreamingQueryProgress`` — how many rows
  the query read, how long its triggers took, how many rows the watermark dropped.
  They describe *this* execution and are gone once the JVM exits.
* **Table metrics** come from reading the Delta table back with ``deltalake`` — no
  JVM, no Spark session. They describe the *state of the world* after the run and
  are what a second run has to reproduce exactly.

The split is the point. Idempotence is a claim about the table, not about the run:
run 2 of ``Trigger.AvailableNow`` legitimately has different run metrics from run 1
(it reads nothing) and must have byte-identical table metrics. Mixing the two into
one number would make the check either vacuous or impossible.

``throughput_rows_per_second`` is computed from Spark's trigger-execution time
rather than wall clock: wall clock on a bounded run is dominated by session
start-up, which says nothing about the pipeline.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ofl.platform.logging import get_logger
from ofl.streaming.paths import metrics_dir, silver_ohlc_dir

log = get_logger(__name__)

#: The silver grain. Two rows sharing it would mean a window was published twice —
#: precisely what the idempotence check exists to rule out.
SILVER_KEY = ("window_start", "symbol")

#: Fields that must match across two consecutive ``AvailableNow`` runs. Deliberately
#: not "the whole dict": ``captured_at`` and the Delta version legitimately differ
#: if a run wrote something, and folding them in would make the check meaningless.
IDEMPOTENT_FIELDS = ("rows", "distinct_keys", "duplicate_keys", "symbols", "total_trades")


def run_metrics(summary: dict) -> dict:
    """Throughput and watermark behaviour for one run, from Spark's own progress.

    ``summary`` is what :func:`ofl.streaming.silver.run_silver_stream` returns.
    """
    progress: list[dict] = summary.get("progress") or []
    input_rows = sum(p.get("numInputRows") or 0 for p in progress)
    duration_ms = sum(p.get("durationMs") or 0 for p in progress)
    return {
        "batches": summary.get("batches", 0),
        "rows_written": summary.get("windows", summary.get("bronze_rows", 0)),
        "input_rows": input_rows,
        "trigger_ms": duration_ms,
        # Guarded: a run with nothing to do has zero duration *and* zero rows, and
        # reporting 0.0 is honest where a ZeroDivisionError would be noise.
        "throughput_rows_per_second": round(input_rows / (duration_ms / 1000), 2)
        if duration_ms
        else 0.0,
        "dropped_late": summary.get("dropped_late", 0),
        "watermark": summary.get("watermark"),
        "open_windows": summary.get("open_windows"),
    }


def silver_table_exists(path: Path | None = None) -> bool:
    """A Delta table is its log — an empty directory is not one."""
    return (Path(path or silver_ohlc_dir()) / "_delta_log").exists()


def read_silver(path: Path | None = None) -> tuple["object", int]:
    """The silver bars as a Polars frame, plus the Delta version they came from.

    The one wrinkle worth naming: Spark writes these timestamps as UTC-stamped, and
    materialising a *tz-aware* timestamp in Python needs an IANA database that is
    not part of the standard library on Windows (``tzdata`` for ``zoneinfo``,
    ``pytz`` for DuckDB). Rather than make the lane depend on a tz database to read
    its own single-timezone table, the columns are cast to naive at the Arrow level.
    The instants are unchanged — Arrow stores tz-aware timestamps as UTC epoch
    micros, so this drops the label, not the meaning. Everything in this lane is
    UTC: the exchange's clock is, and the bars are floored on the UTC epoch.
    """
    import polars as pl
    import pyarrow as pa
    from deltalake import DeltaTable

    dt = DeltaTable(str(Path(path or silver_ohlc_dir())))
    arrow = dt.to_pyarrow_table()
    naive = pa.schema(
        [f.with_type(pa.timestamp("us")) if pa.types.is_timestamp(f.type) else f for f in arrow.schema]
    )
    return pl.from_arrow(arrow.cast(naive)), dt.version()


def table_metrics(path: Path | None = None) -> dict:
    """Read the silver Delta table back and describe it. No Spark, no JVM.

    Reading with ``deltalake`` rather than the session that just wrote the table is
    what makes this evidence: it is an independent reader observing committed state,
    not the writer reporting on itself.
    """
    if not silver_table_exists(path):
        return {"exists": False, "rows": 0, "distinct_keys": 0, "duplicate_keys": 0}

    bars, version = read_silver(path)
    rows = bars.height
    distinct_keys = bars.select(list(SILVER_KEY)).n_unique() if rows else 0
    return {
        "exists": True,
        "rows": rows,
        "distinct_keys": distinct_keys,
        # The reconciliation column: append mode publishes a window once, so any
        # value but zero means the sink's idempotency guard failed.
        "duplicate_keys": rows - distinct_keys,
        "symbols": bars["symbol"].n_unique() if rows else 0,
        # Sums to the number of bronze events the published bars were built from —
        # the column that ties silver back to its source.
        "total_trades": int(bars["trades"].sum()) if rows else 0,
        "min_window_start": _iso(bars["window_start"].min()) if rows else None,
        "max_window_end": _iso(bars["window_end"].max()) if rows else None,
        # Event time, not processing time: the newest trade any published bar saw.
        "max_event_time": _iso(bars["last_trade_time"].max()) if rows else None,
        "delta_version": version,
    }


def snapshot(summary: dict | None = None, *, mode: str, path: Path | None = None) -> dict:
    """A full per-run snapshot: what the run did, and what the table looks like after."""
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mode": mode,
        "table": "silver/fact_trade_ohlc_1m",
        "run": run_metrics(summary) if summary is not None else None,
        "state": table_metrics(path),
    }


def write_snapshot(snap: dict, *, name: str, directory: Path | None = None) -> Path:
    """Persist a snapshot as JSON under ``data/streaming/_metrics/`` (gitignored).

    Generated data lives with the rest of the generated data; the copies that make
    it into ``docs/`` are committed by hand, as evidence of a specific run.
    """
    out_dir = Path(directory or metrics_dir())
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{name}.json"
    out.write_text(json.dumps(snap, indent=2, default=str) + "\n", encoding="utf-8")
    log.info("metrics_snapshot", path=str(out), **{k: v for k, v in snap["state"].items()})
    return out


def compare_states(first: dict, second: dict) -> dict[str, tuple[Any, Any]]:
    """Fields of two table snapshots that disagree — empty dict means idempotent.

    Returns the differences rather than a bool so a failure names what moved.
    """
    return {
        field: (first.get(field), second.get(field))
        for field in IDEMPOTENT_FIELDS
        if first.get(field) != second.get(field)
    }


def _iso(value) -> str | None:
    if value is None:
        return None
    return value.isoformat(sep=" ") if isinstance(value, datetime) else str(value)
