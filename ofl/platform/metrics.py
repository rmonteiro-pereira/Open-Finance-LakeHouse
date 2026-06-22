"""Per-series operational metrics, pushed to a Prometheus Pushgateway.

Ingest pods are short-lived KubernetesPodOperator pods — Prometheus can't scrape
them before they exit — so they *push* their state to a Pushgateway, which
Prometheus scrapes. Alertmanager then turns the metrics into individual,
per-series e-mail alerts (failure / data-quality / freshness).

Everything here is best-effort and env-gated on ``OFL_PUSHGATEWAY_URL``: with no
gateway configured (local dev, tests) every call is a no-op, so ingestion never
depends on the metrics backend. Only the stdlib is used, so this module is safe
to import from the lightweight Airflow worker context (failure callbacks) as well
as from inside the ingest image.

Metric model — uniform label set ``{series, source, domain, cadence}`` so the
series key is the universal join across Prometheus, OpenMetadata (bronze/{series}
table) and the OpenLineage job name:

    ofl_series_last_success_timestamp_seconds   latest landed *data* date (not wall-clock)
    ofl_dq_passed / ofl_dq_failed               1/0 — last contract verdict for the series
    ofl_ingest_failed                           1/0 — last run failed after retries

The grouping key is just ``series`` (one Pushgateway group per series). Pushes use
POST so same-named metrics are replaced while the others in the group survive — a
later success POST resets the failure/dq gauges to 0 without clobbering the
freshness timestamp.
"""

from __future__ import annotations

import os
import urllib.request
from typing import TYPE_CHECKING

from ofl.platform.logging import get_logger

if TYPE_CHECKING:
    from ofl.registry import Series

log = get_logger(__name__)

_JOB = "ofl_ingest"
_ENV = "OFL_PUSHGATEWAY_URL"


def _labels(series: Series) -> dict[str, str]:
    return {
        "source": series.handler,
        "domain": series.domain,
        "cadence": series.frequency,
    }


def _line(name: str, value: float, labels: dict[str, str]) -> str:
    rendered = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{rendered}}} {value}\n"


def _push(series_key: str, body: str) -> None:
    """POST a Prometheus text body to the gateway under group ``series=<key>``."""
    base = os.getenv(_ENV)
    if not base:
        return
    url = f"{base.rstrip('/')}/metrics/job/{_JOB}/series/{series_key}"
    req = urllib.request.Request(
        url, data=body.encode("utf-8"), method="POST",
        headers={"Content-Type": "text/plain; version=0.0.4"},
    )
    try:
        urllib.request.urlopen(req, timeout=5).close()  # noqa: S310 - in-cluster gateway
    except Exception as exc:  # noqa: BLE001 - metrics are best-effort
        log.warning("pushgateway_push_failed", series=series_key, error=str(exc))


def record_ingest_success(series: Series, *, last_obs_epoch: float | None) -> None:
    """Mark the series healthy: refresh freshness, clear failure/dq gauges."""
    labels = _labels(series)
    body = _line("ofl_dq_passed", 1, labels)
    body += _line("ofl_dq_failed", 0, labels)
    body += _line("ofl_ingest_failed", 0, labels)
    if last_obs_epoch is not None:
        body += _line("ofl_series_last_success_timestamp_seconds", last_obs_epoch, labels)
    _push(series.key, body)


def record_dq_failure(series: Series, *, check: str, breaches: int = 0) -> None:
    """Flag a data-quality contract violation (task will fail; bronze withheld)."""
    labels = {**_labels(series), "check": check}
    body = _line("ofl_dq_failed", 1, labels)
    body += _line("ofl_dq_passed", 0, _labels(series))
    if breaches:
        body += _line("ofl_dq_rows_failed", breaches, labels)
    _push(series.key, body)


def record_ingest_failure(series: Series, *, reason: str) -> None:
    """Flag an ingest run failure (from the Airflow on_failure_callback)."""
    labels = {**_labels(series), "reason": reason}
    _push(series.key, _line("ofl_ingest_failed", 1, labels))


def record_ingest_failure_raw(series_key: str, *, reason: str) -> None:
    """Failure path when the series can't be resolved against the registry."""
    labels = {"source": "unknown", "domain": "unknown", "cadence": "unknown", "reason": reason}
    _push(series_key, _line("ofl_ingest_failed", 1, labels))
