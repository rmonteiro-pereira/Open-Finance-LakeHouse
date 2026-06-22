"""Yahoo Finance extractor (Polars) — second handler family, ``security_price`` fact.

Demonstrates the registry-driven handler pattern generalizing beyond BACEN: a
grouped source (``yahoo_etf`` / ``yahoo_commodity`` / ``yahoo_currency``) fans out
over its ``symbols`` and lands a long OHLCV frame.
"""

from __future__ import annotations

import polars as pl

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

_OHLCV = ["open", "high", "low", "close", "volume"]
_EMPTY_SCHEMA = {"symbol": pl.String, "date": pl.Date, **{c: pl.Float64 for c in _OHLCV}}


def _to_long(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    """Normalize a single-symbol OHLCV frame to ``(symbol, date, o, h, l, c, v)``."""
    renamed = df.rename({c: c.lower().replace(" ", "_") for c in df.columns})
    present = [c for c in _OHLCV if c in renamed.columns]
    return (
        renamed.with_columns(
            pl.col("date").cast(pl.Date),
            pl.lit(symbol).alias("symbol"),
            *[pl.col(c).cast(pl.Float64) for c in present],
        )
        .select("symbol", "date", *present)
    )


def fetch_yahoo(symbols: list[str], *, period: str = "max") -> pl.DataFrame:
    import yfinance as yf

    frames = []
    for sym in symbols:
        raw = yf.download(sym, period=period, auto_adjust=False, progress=False, multi_level_index=False)
        if raw is None or raw.empty:
            log.warning("yahoo_empty", symbol=sym)
            continue
        frames.append(_to_long(pl.from_pandas(raw.reset_index()), sym))

    if not frames:
        return pl.DataFrame(schema=_EMPTY_SCHEMA)
    return pl.concat(frames, how="vertical_relaxed")


def ingest_yahoo(series: Series) -> dict:
    symbols = [s["symbol"] for s in series.symbols]
    df = fetch_yahoo(symbols)
    log.info("yahoo_fetched", series=series.key, symbols=len(symbols), rows=df.height)
    return land_bronze(series, df)
