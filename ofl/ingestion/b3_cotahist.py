"""B3 COTAHIST extractor — official historical cash-market quotes (Polars).

The authoritative, free source of daily OHLCV for every B3-listed equity: a
fixed-width, positional, Latin-1 file (245 bytes/record) published as annual /
monthly / daily ZIPs. Distinct from the ``b3`` handler (Ibovespa *index* via
Yahoo) — this is per-security cash-market data straight from the exchange.

``fact: security_price`` (same lane as Yahoo): lands a long OHLCV frame in bronze
and is consumed directly by gold (no silver MERGE).

Record layout (TIPREG=01), 1-based positions per the official B3 layout PDF, here
as 0-based ``str.slice(offset, length)``. Prices carry 2 implied decimals (/100);
VOLTOT likewise. We keep round-lot cash quotes only: TPMERC=010 + CODBDI=02.
"""

from __future__ import annotations

import io
import zipfile
from datetime import date

import polars as pl
import requests

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

# Legacy static host still serves the files; the www.b3.com.br page just links here.
BASE_URL = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist"
# Bare scripted requests are sometimes 403'd — send a browser-like UA.
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ofl-lakehouse/1.0)"}

_FINAL_COLS = ["symbol", "date", "open", "high", "low", "close", "volume", "volume_brl", "trades", "fatcot"]
_EMPTY = {
    "symbol": pl.String, "date": pl.Date,
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
    "volume": pl.Int64, "volume_brl": pl.Float64, "trades": pl.Int64, "fatcot": pl.Int64,
}


def _file_names(extra: dict) -> list[str]:
    """Resolve the ZIP file name(s) for this series from its registry ``extra``.

    ``years: [..]`` -> annual backfill (one file per year). Otherwise a single
    daily file for ``day`` (``DDMMAAAA``) or today.
    """
    years = extra.get("years")
    if years:
        return [f"COTAHIST_A{y}.ZIP" for y in years]
    if extra.get("month"):
        return [f"COTAHIST_M{extra['month']}.ZIP"]  # MMAAAA
    day = extra.get("day") or date.today().strftime("%d%m%Y")  # DDMMAAAA
    return [f"COTAHIST_D{day}.ZIP"]


def _download(name: str) -> bytes:
    resp = requests.get(f"{BASE_URL}/{name}", headers=_HEADERS, timeout=180)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        return z.read(z.namelist()[0])  # one .TXT per ZIP


def parse_cotahist(raw: bytes) -> pl.DataFrame:
    """Parse a COTAHIST .TXT (bytes) into a typed quote frame (all record-01 rows).

    Latin-1 is single-byte, so each line is exactly 245 chars and char offsets ==
    byte offsets; slicing stays aligned even on accented (unused) name fields.
    """
    lines = [ln for ln in raw.decode("latin-1").splitlines() if ln[:2] == "01"]
    if not lines:
        return pl.DataFrame(schema={**_EMPTY, "codbdi": pl.String, "tpmerc": pl.String})
    return pl.DataFrame({"raw": lines}).select(
        pl.col("raw").str.slice(2, 8).str.to_date("%Y%m%d").alias("date"),
        pl.col("raw").str.slice(10, 2).alias("codbdi"),
        pl.col("raw").str.slice(12, 12).str.strip_chars().alias("symbol"),
        pl.col("raw").str.slice(24, 3).alias("tpmerc"),
        (pl.col("raw").str.slice(56, 13).cast(pl.Float64) / 100).alias("open"),
        (pl.col("raw").str.slice(69, 13).cast(pl.Float64) / 100).alias("high"),
        (pl.col("raw").str.slice(82, 13).cast(pl.Float64) / 100).alias("low"),
        (pl.col("raw").str.slice(108, 13).cast(pl.Float64) / 100).alias("close"),
        pl.col("raw").str.slice(147, 5).cast(pl.Int64).alias("trades"),
        pl.col("raw").str.slice(152, 18).cast(pl.Int64).alias("volume"),       # QUATOT (shares)
        (pl.col("raw").str.slice(170, 18).cast(pl.Float64) / 100).alias("volume_brl"),  # VOLTOT
        pl.col("raw").str.slice(210, 7).cast(pl.Int64).alias("fatcot"),
    )


def ingest_b3_cotahist(series: Series) -> dict:
    extra = series.extra
    names = _file_names(extra)
    frames = [parse_cotahist(_download(n)) for n in names]
    df = pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame(schema=_EMPTY)

    df = df.filter(
        pl.col("codbdi").is_in(extra.get("codbdi", ["02"]))
        & pl.col("tpmerc").is_in(extra.get("tpmerc", ["010"]))
    )
    tickers = extra.get("tickers")
    if tickers:
        df = df.filter(pl.col("symbol").is_in(tickers))
    df = df.select(_FINAL_COLS).unique(subset=["symbol", "date"], keep="last").sort("symbol", "date")

    # Annual backfill replaces the table; a daily pull appends the new day.
    mode = "overwrite" if extra.get("years") else "append"
    log.info("b3_cotahist_fetched", series=series.key, files=len(names), rows=df.height, mode=mode)
    return land_bronze(series, df, mode=mode)
