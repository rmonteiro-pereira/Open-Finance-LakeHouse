"""BACEN / SGS extractor (Polars).

Replaces the old pandas ``utils/bacen_api.py``: vectorized parse, idempotent
de-dup across the overlapping 10-year windows the SGS API forces on daily series.
Covers every ``bacen_sgs`` series in the registry (rates, inflation, fx, fiscal).
"""

from __future__ import annotations

import time
from datetime import date, timedelta

import polars as pl
import requests

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

SGS_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
_MIN_YEAR = 1900
_BR_DATE = "%d/%m/%Y"


def _get_window(series_id: int, start: date, end: date) -> tuple[str, list | None]:
    """Fetch one SGS window. Returns ``(status, payload)`` where status is
    ``ok`` (data), ``empty`` (no data in range — 200 ``[]`` or 404), or
    ``error`` (transient: 5xx / timeout / non-JSON)."""
    params = {
        "formato": "json",
        "dataInicial": start.strftime(_BR_DATE),
        "dataFinal": end.strftime(_BR_DATE),
    }
    try:
        resp = requests.get(SGS_URL.format(series_id=series_id), params=params, timeout=60)
    except requests.RequestException as exc:
        log.warning("sgs_window_error", series_id=series_id, error=str(exc))
        return "error", None

    if resp.status_code == 404:
        return "empty", None  # SGS returns 404 for ranges with no data (before inception)
    if resp.status_code >= 500:
        return "error", None
    try:
        resp.raise_for_status()
        payload = resp.json()
    except (requests.HTTPError, ValueError) as exc:
        log.warning("sgs_window_error", series_id=series_id, error=str(exc))
        return "error", None
    return ("ok", payload) if payload else ("empty", None)


def fetch_sgs(
    series_id: int,
    *,
    window_years: int = 10,
    end: date | None = None,
    since: date | None = None,
    max_retries: int = 2,
) -> pl.DataFrame:
    """Fetch an SGS series as a tidy ``(date, value)`` Polars frame.

    SGS caps daily series at ~10y per request, so we walk backwards in windows.
    Once we have data and hit an empty window we've passed inception and stop —
    no blind shrinking back to 1900. Transient errors retry the same window.

    ``since`` floors the walk (bounded backfill / incremental loads); ``None``
    fetches full history.
    """
    end = end or date.today()
    floor = since or date(_MIN_YEAR, 1, 1)
    frames: list[pl.DataFrame] = []
    have_data = False
    retries = 0

    while end >= floor:
        start = end.replace(year=max(floor.year, end.year - window_years + 1))
        if start < floor:
            start = floor
        status, payload = _get_window(series_id, start, end)

        if status == "ok":
            # SGS sometimes adds a `datafim` column; keep only (data, valor) as
            # strings so windows concat cleanly regardless of shape.
            frames.append(
                pl.DataFrame(payload).select(
                    pl.col("data").cast(pl.Utf8),
                    pl.col("valor").cast(pl.Utf8),
                )
            )
            have_data = True
            retries = 0
            log.info("sgs_window", series_id=series_id, rows=len(payload), start=str(start), end=str(end))
            end = start - timedelta(days=1)
        elif status == "empty":
            if have_data:
                break  # walked past the series' first observation
            end = start - timedelta(days=1)  # late-starting series: keep scanning back
        else:  # transient error: polite backoff, then retry same window
            if retries < max_retries:
                retries += 1
                time.sleep(min(2 * retries, 6))
                continue
            retries = 0
            end = start - timedelta(days=1)

    if not frames:
        return pl.DataFrame(schema={"date": pl.Date, "value": pl.Float64})

    return (
        pl.concat(frames, how="vertical_relaxed")
        .select(
            pl.col("data").str.to_date(_BR_DATE).alias("date"),
            pl.col("valor").cast(pl.Float64, strict=False).alias("value"),
        )
        .drop_nulls("date")
        .unique(subset="date", keep="last")  # idempotent across overlapping windows
        .sort("date")
    )


def ingest_bacen_sgs(series: Series) -> dict:
    if series.sgs_id is None:
        raise ValueError(f"series '{series.key}' has handler bacen_sgs but no sgs_id")
    df = fetch_sgs(series.sgs_id)
    log.info("sgs_fetched", series=series.key, sgs_id=series.sgs_id, rows=df.height)
    return land_bronze(series, df)
