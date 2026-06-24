"""B3 public market-data portal extractor — ``arquivos.b3.com.br`` (Polars).

The free, public subset of B3's market-data files (regulator-mandated): daily
**CSV**, ``;``-separated, Latin-1, decimal comma. This is the modern channel that
replaced most of the legacy positional/BDI files. Three files are wired here, all
free (no auth, no recaptcha):

- ``DerivativesOpenPosition``     -> ``fact: open_interest``    (OI per contract)
- ``TradeInformationConsolidated``-> ``fact: derivatives_quote`` (prices + settlement)
- ``InstrumentsConsolidated``     -> ``fact: instrument``        (instrument snapshot/dim)

Download is a two-step handshake: ``requestname`` returns a one-shot token, then
``/api/download/?token=`` streams the file. A missing date (holiday / not yet
published) returns HTTP 400 with no token — those dates are skipped, not fatal.

Date window (per series ``extra`` or the ``OFL_B3_WINDOW`` env override):
- ``OFL_B3_WINDOW="YYYY-MM-DD..YYYY-MM-DD"`` (env) or ``start:``/``end:`` (registry)
  -> walk every weekday in the inclusive range (one-off backfill, overwrite).
- otherwise -> the last ``lookback_days`` weekdays (steady-state daily, append).

``LoanBalance``/``LendingOpenPosition`` (securities lending / BTB) are NOT wired:
they 400 on the public endpoint (moved to the paid Up2Data tier).
"""

from __future__ import annotations

import io
import os
import time
from datetime import date, timedelta

import polars as pl
import requests

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

BASE_URL = "https://arquivos.b3.com.br/api/download"
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ofl-lakehouse/1.0)"}
# Futures-only by default: the index/rate (FINANCIAL: DI1, DOL, IND, WIN, WDO...)
# and commodity (AGRIBUSINESS: boi, milho, café, soja, etanol) contracts. Equity
# single-stock options (EQUITY CALL/PUT) are excluded by default — they multiply
# the row count ~25x; add them to a series' `segments` list to opt in.
_DEFAULT_SEGMENTS = ["FINANCIAL", "AGRIBUSINESS"]


# --------------------------------------------------------------------------- io
def _download(file_name: str, day: date, *, retries: int = 3) -> bytes | None:
    """Fetch one daily file via the token handshake. ``None`` if unavailable.

    Resilient by design — over a multi-month walk the portal intermittently
    returns 401 (a token races/expires) or times out. Such transients are retried
    with linear backoff and, if still failing, the date is **skipped** (``None``)
    rather than raising, so one bad response never aborts the whole backfill. A
    genuine "no data" (non-200 requestname, missing token, empty body) skips at once.
    """
    iso = day.isoformat()
    for attempt in range(retries):
        try:
            r = requests.get(
                f"{BASE_URL}/requestname",
                params={"fileName": file_name, "date": iso},
                headers=_HEADERS,
                timeout=120,
            )
            if r.status_code != 200:
                return None
            token = (r.json() or {}).get("token")
            if not token:
                return None
            f = requests.get(f"{BASE_URL}/", params={"token": token}, headers=_HEADERS, timeout=300)
            if f.status_code != 200:  # 401/5xx — transient; retry then skip
                if attempt < retries - 1:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                return None
            # Some dates return HTTP 200 with an EMPTY body (token issued but no
            # file exists, e.g. older than the portal's ~12-month retention) — skip.
            if not f.content or not f.content.strip():
                return None
            return f.content
        except requests.RequestException:
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            return None
    return None


def _read_csv(raw: bytes) -> pl.DataFrame:
    """Parse a B3 portal CSV (``;``-sep, Latin-1) with all columns as strings.

    Some files carry a ``Status do Arquivo: Final`` preamble line before the
    header — skipped by detecting the real header row (starts with ``RptDt``).
    """
    text = raw.decode("latin-1")
    skip = 0 if text.lstrip().startswith("RptDt") else 1
    return pl.read_csv(
        io.BytesIO(text.encode("utf-8")),
        separator=";",
        skip_rows=skip,
        has_header=True,
        infer_schema_length=0,  # every column as Utf8; we cast explicitly below
        truncate_ragged_lines=True,
    )


def _num_simple(col: str) -> pl.Expr:
    """B3 decimal-comma string -> Float64 (empty -> null). B3 omits thousands
    separators, so only the decimal comma needs swapping for a dot."""
    return pl.col(col).str.replace(",", ".", literal=True).cast(pl.Float64, strict=False)


def _to_date(col: str) -> pl.Expr:
    return pl.col(col).str.to_date("%Y-%m-%d", strict=False)


# ----------------------------------------------------------------- parsers ---
def parse_open_position(raw: bytes) -> pl.DataFrame:
    df = _read_csv(raw)
    return df.select(
        _to_date("RptDt").alias("date"),
        pl.col("TckrSymb").alias("symbol"),
        pl.col("ISIN").alias("isin"),
        pl.col("Asst").alias("asset"),
        pl.col("XprtnCd").alias("expiration_code"),
        pl.col("SgmtNm").alias("segment"),
        _num_simple("OpnIntrst").alias("open_interest"),
        _num_simple("VartnOpnIntrst").alias("open_interest_var"),
    )


def parse_trade_information(raw: bytes) -> pl.DataFrame:
    df = _read_csv(raw)
    return df.select(
        _to_date("RptDt").alias("date"),
        pl.col("TckrSymb").alias("symbol"),
        pl.col("ISIN").alias("isin"),
        pl.col("SgmtNm").alias("segment"),
        _num_simple("MinPric").alias("min_price"),
        _num_simple("MaxPric").alias("max_price"),
        _num_simple("TradAvrgPric").alias("avg_price"),
        _num_simple("LastPric").alias("last_price"),
        _num_simple("OscnPctg").alias("oscillation_pct"),
        _num_simple("AdjstdQt").alias("settlement_price"),    # PU de ajuste (futuros)
        _num_simple("AdjstdQtTax").alias("settlement_rate"),  # taxa de ajuste (DI/DAP...)
        _num_simple("RefPric").alias("ref_price"),
        _num_simple("TradQty").alias("trades"),
        _num_simple("FinInstrmQty").alias("contracts"),
        _num_simple("NtlFinVol").alias("notional"),
    )


def parse_instruments(raw: bytes) -> pl.DataFrame:
    df = _read_csv(raw)
    return df.select(
        _to_date("RptDt").alias("date"),
        pl.col("TckrSymb").alias("symbol"),
        pl.col("Asst").alias("asset"),
        pl.col("AsstDesc").alias("asset_desc"),
        pl.col("SgmtNm").alias("segment"),
        pl.col("MktNm").alias("market"),
        pl.col("SctyCtgyNm").alias("security_category"),
        _to_date("XprtnDt").alias("expiration_date"),
        pl.col("XprtnCd").alias("expiration_code"),
        pl.col("ISIN").alias("isin"),
        pl.col("CFICd").alias("cfi_code"),
        pl.col("OptnTp").alias("option_type"),
        _num_simple("CtrctMltplr").alias("contract_multiplier"),
        _num_simple("AllcnRndLot").alias("round_lot"),
        pl.col("TradgCcy").alias("trading_ccy"),
        _num_simple("ExrcPric").alias("strike"),
        pl.col("OptnStyle").alias("option_style"),
        pl.col("UndrlygTckrSymb1").alias("underlying"),
    )


_PARSERS = {
    "DerivativesOpenPosition": parse_open_position,
    "TradeInformationConsolidated": parse_trade_information,
    "InstrumentsConsolidated": parse_instruments,
}


# ------------------------------------------------------------- date window ---
def _weekdays(start: date, end: date) -> list[date]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri (holidays just 400 and are skipped)
            days.append(d)
        d += timedelta(days=1)
    return days


def _resolve_window(extra: dict, *, allow_env: bool = True) -> tuple[list[date], bool]:
    """Return (dates, is_backfill). Backfill walks an explicit inclusive range.

    ``allow_env=False`` ignores the ``OFL_B3_WINDOW`` backfill override — used by
    the instrument snapshot, which must always pull just the latest day even while
    a range backfill of the fact tables is in flight.
    """
    env = os.environ.get("OFL_B3_WINDOW", "") if allow_env else ""
    start = end = None
    if ".." in env:
        a, b = env.split("..", 1)
        start, end = date.fromisoformat(a.strip()), date.fromisoformat(b.strip())
    elif allow_env and extra.get("start"):
        start = date.fromisoformat(str(extra["start"]))
        end = date.fromisoformat(str(extra["end"])) if extra.get("end") else date.today()
    if start:
        return _weekdays(start, end), True

    # Steady-state: the last `lookback_days` weekdays ending today (inclusive).
    lookback = int(extra.get("lookback_days", 3))
    days, d = [], date.today()
    while len(days) < lookback:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return sorted(days), False


# ----------------------------------------------------------------- handler ---
def ingest_b3_arquivos(series: Series) -> dict:
    extra = series.extra
    file_name = extra["file"]
    parser = _PARSERS[file_name]

    # The instrument file is a dimension: only the latest snapshot is meaningful,
    # so we never walk a range — fetch the most recent available day and overwrite.
    instruments = file_name == "InstrumentsConsolidated"
    if instruments:
        # Snapshot dimension: look back a few weekdays (not just today, whose file
        # may not be published yet) and keep the latest — ignoring OFL_B3_WINDOW /
        # start-end so it never walks the multi-month backfill range.
        dates, is_backfill = _resolve_window(
            {"lookback_days": extra.get("lookback_days", 3)}, allow_env=False
        )
    else:
        dates, is_backfill = _resolve_window(extra)

    frames: list[pl.DataFrame] = []
    fetched = 0
    skipped = 0
    for d in dates:
        raw = _download(file_name, d)
        if raw is None:
            continue
        try:
            frames.append(parser(raw))
        except Exception as exc:  # noqa: BLE001 - one malformed file must not abort a multi-year walk
            skipped += 1
            log.warning("b3_arquivos_parse_skip", series=series.key, date=d.isoformat(), error=str(exc))
            continue
        fetched += 1

    if not frames:  # every date was a holiday / not yet published — nothing to land
        log.warning("b3_arquivos_no_files", series=series.key, file=file_name, dates=len(dates))
        return {"series": series.key, "skipped": True, "reason": "no_files"}

    df = pl.concat(frames, how="vertical_relaxed")

    # Keep only the configured segments (futures by default) for the fact tables;
    # the instrument dimension keeps every segment so any fact symbol resolves.
    if not instruments:
        segments = extra.get("segments", _DEFAULT_SEGMENTS)
        df = df.filter(pl.col("segment").is_in(segments))

    # Grain hygiene: one row per (symbol, date), latest wins within the run.
    df = df.unique(subset=["symbol", "date"], keep="last").sort("symbol", "date")

    # Snapshot dim + range backfill replace the table; steady-state daily appends.
    mode = "overwrite" if (instruments or is_backfill) else "append"
    log.info(
        "b3_arquivos_fetched",
        series=series.key,
        file=file_name,
        dates=len(dates),
        files=fetched,
        skipped=skipped,
        rows=df.height,
        mode=mode,
    )
    return land_bronze(series, df, mode=mode)
