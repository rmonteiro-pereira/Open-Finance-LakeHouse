"""Airflow failure callback -> per-series ingest-failure metric.

Wired as ``on_failure_callback`` on every ingest task. It runs in the Airflow
worker/scheduler context (NOT inside the pod), so it still fires when the pod
itself dies (OOM/137, image-pull) before it could push anything. Fires only on
FINAL failure (after retries), so a transient API 429 that self-heals is silent.

The task_id is ``ingest_<series>``, so the series is recovered directly — no
map-index bookkeeping (static per-series tasks, not dynamic mapping).
"""

from __future__ import annotations

from ofl.platform.logging import get_logger

log = get_logger("ofl.airflow.alerts")

_PREFIX = "ingest_"


def ofl_failure_alert(context) -> None:
    ti = context.get("task_instance") or context.get("ti")
    task_id = getattr(ti, "task_id", "") or ""
    exc = context.get("exception")
    reason = type(exc).__name__ if exc else "failed"
    series_key = task_id[len(_PREFIX):] if task_id.startswith(_PREFIX) else task_id
    try:
        from ofl.platform.metrics import record_ingest_failure, record_ingest_failure_raw
        from ofl.registry import load_registry

        series = load_registry().series.get(series_key)
        if series is not None:
            record_ingest_failure(series, reason=reason)
        else:
            record_ingest_failure_raw(series_key, reason=reason)
    except Exception as exc:  # noqa: BLE001 - a callback must never raise
        log.warning("failure_callback_error", task_id=task_id, error=str(exc))
