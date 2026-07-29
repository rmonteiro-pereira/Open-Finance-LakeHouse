"""Streaming lane — a live Spark Structured Streaming path alongside the batch lanes.

The batch lanes (Polars extract → Spark silver → DuckDB gold) run on a schedule
over closed periods. This lane runs continuously over a live market feed:

    Binance public WS  →  ``_landing/*.jsonl``  →  Spark Structured Streaming
                                                   →  bronze Delta (+ dead letter)
                                                   →  silver Delta (1-min OHLC bars)

Milestone status: **M0 (producer), M1 (bronze stream), M2 (event-time silver),
M3 (``Trigger.AvailableNow`` with a measured idempotence check) and M4's metrics
snapshot are implemented.** The R2 backend that would make the committed
``workflow_dispatch`` workflow live is human-gated; see ``docs/STREAMING.md``.
"""
