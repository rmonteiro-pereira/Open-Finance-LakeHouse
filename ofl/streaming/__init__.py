"""Streaming lane — a live Spark Structured Streaming path alongside the batch lanes.

The batch lanes (Polars extract → Spark silver → DuckDB gold) run on a schedule
over closed periods. This lane runs continuously over a live market feed:

    Binance public WS  →  ``_landing/*.jsonl``  →  Spark Structured Streaming
                                                   →  bronze Delta (+ dead letter)
                                                   →  silver Delta (1-min OHLC bars)
                                                   →  near-real-time DuckDB mart

Milestone status: **M0–M5 are implemented** — producer, bronze stream, event-time
silver, ``Trigger.AvailableNow`` with a measured idempotence check, per-run metrics
snapshots, and the served mart. The R2 backend that would make the committed
``workflow_dispatch`` workflow *live* is human-gated; see ``docs/STREAMING.md``.
"""
