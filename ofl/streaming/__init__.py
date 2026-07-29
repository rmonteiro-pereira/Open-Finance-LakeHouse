"""Streaming lane — a live Spark Structured Streaming path alongside the batch lanes.

The batch lanes (Polars extract → Spark silver → DuckDB gold) run on a schedule
over closed periods. This lane runs continuously over a live market feed:

    Binance public WS  →  ``_landing/*.jsonl``  →  Spark Structured Streaming
                                                   →  bronze Delta (+ dead letter)
                                                   →  silver Delta (1-min OHLC bars)

Milestone status: **M0 (producer), M1 (bronze stream) and M2 (event-time silver)
are implemented.** The ``Trigger.AvailableNow`` cron mode and the R2 backend are
M3+; see ``docs/STREAMING.md``.
"""
