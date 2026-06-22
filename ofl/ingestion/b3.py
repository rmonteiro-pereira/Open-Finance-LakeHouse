"""B3 index extractor — B3 index levels sourced via Yahoo Finance.

B3 has no free official market-data API (the official feed is paid/authorized),
so B3 index time series (Ibovespa ``^BVSP`` and other B3 indices Yahoo carries)
are sourced through Yahoo, which publishes the real index values. The handler is
kept distinct from ``yahoo`` so ``dim_series.source`` reads ``b3``.
"""

from __future__ import annotations

from ofl.ingestion.landing import land_bronze
from ofl.ingestion.yahoo import fetch_yahoo
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)


def ingest_b3(series: Series) -> dict:
    symbols = [s["symbol"] for s in series.symbols]
    df = fetch_yahoo(symbols)
    log.info("b3_fetched", series=series.key, symbols=len(symbols), rows=df.height)
    return land_bronze(series, df)
