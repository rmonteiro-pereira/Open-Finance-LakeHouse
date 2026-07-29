"""Streaming lane — a live Spark Structured Streaming path alongside the batch lanes.

The batch lanes (Polars extract → Spark silver → DuckDB gold) run on a schedule
over closed periods. This lane runs continuously over a live market feed:

    Binance public WS  →  ``_landing/*.jsonl``  →  Spark Structured Streaming
                                                   →  bronze Delta (+ dead letter)

Milestone status: **M0 (producer) and M1 (bronze stream) are implemented.** The
event-time/watermark aggregation to silver, the ``Trigger.AvailableNow`` cron mode
and the R2 backend are M2+; see ``docs/STREAMING.md``.
"""
