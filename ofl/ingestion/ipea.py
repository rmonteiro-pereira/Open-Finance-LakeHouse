"""IPEA (ipeadata) extractor — real public OData4 API, ``observation`` fact.

No authentication. Each registry series carries a real ``sercodigo``; values come
from ``ValoresSerie(SERCODIGO=...)`` as ``{VALDATA, VALVALOR}``.

Replaces the legacy ``ipea_receita`` handler, which returned ``random`` mock data.
"""

from __future__ import annotations

import polars as pl
import requests

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

ODATA = "http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{code}')"


def _parse(rows: list) -> pl.DataFrame:
    """Flatten IPEA ValoresSerie rows to ``(date, value)``.

    Build only the two columns we need; ``VALVALOR`` mixes int/float across rows,
    so pin the dtype rather than rely on inference.
    """
    if not rows:
        return pl.DataFrame(schema={"date": pl.Date, "value": pl.Float64})
    frame = pl.DataFrame(
        {
            "date": [str(r.get("VALDATA", ""))[:10] for r in rows],
            "value": [r.get("VALVALOR") for r in rows],
        },
        schema={"date": pl.Utf8, "value": pl.Float64},
    )
    return (
        frame.with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        .drop_nulls("date")
        .unique("date", keep="last")
        .sort("date")
    )


def fetch_ipea(sercodigo: str) -> pl.DataFrame:
    resp = requests.get(ODATA.format(code=sercodigo), timeout=120)
    resp.raise_for_status()
    return _parse(resp.json().get("value", []))


def ingest_ipea(series: Series) -> dict:
    sercodigo = series.extra.get("sercodigo")
    if not sercodigo:
        raise ValueError(f"series '{series.key}' has handler ipea but no sercodigo")
    df = fetch_ipea(sercodigo)
    log.info("ipea_fetched", series=series.key, sercodigo=sercodigo, rows=df.height)
    return land_bronze(series, df)
