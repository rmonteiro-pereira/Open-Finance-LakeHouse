#!/usr/bin/env python3
"""Export LIVE OFL gold marts from MinIO (Delta Lake) to dashboard JSON.

Produces the same files as `gen_synthetic.py`, so the dashboard is agnostic to
the source. Reads MinIO credentials from the environment (k8s secret
`minio-creds` when run in-cluster, or `kubectl port-forward` + env locally).

Env:
    MINIO_ENDPOINT   default http://minio.minio.svc.cluster.local:9000
    MINIO_USER       (required)  -> from secret key MINIO_USER
    MINIO_PASSWORD   (required)  -> from secret key MINIO_PASSWORD
    BUCKET           default lakehouse

    python snapshot/export.py

Notes:
- DuckDB's delta_scan reads S3 creds from the *secret manager* (CREATE SECRET),
  NOT the legacy SET s3_* vars. Without it, it falls through to the AWS default
  chain and HANGS on the EC2 metadata endpoint inside the cluster. See
  docs/DASHBOARD_HANDOFF.md s2.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from urllib.parse import urlparse

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed.  pip install duckdb  (>=0.10 with delta extension)")

HERE = os.path.dirname(os.path.abspath(__file__))
# DASH_DATA_DIR lets the cluster CronJob write straight to the shared volume.
OUT = os.environ.get("DASH_DATA_DIR") or os.path.normpath(os.path.join(HERE, "..", "public", "data"))
os.makedirs(OUT, exist_ok=True)

ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
USER = os.environ.get("MINIO_USER")
PASSWORD = os.environ.get("MINIO_PASSWORD")
BUCKET = os.environ.get("BUCKET", "lakehouse")

if not (USER and PASSWORD):
    sys.exit("MINIO_USER / MINIO_PASSWORD must be set (mount secret `minio-creds`).")

parsed = urlparse(ENDPOINT)
host_port = parsed.netloc or parsed.path  # host:port, no scheme
use_ssl = "true" if parsed.scheme == "https" else "false"

# gold marts dumped verbatim (small / monthly) -> output file name
MARTS = [
    "mart_macro_dashboard",
    "mart_real_interest",
    "mart_inflation_panel",
    "mart_fx",
]

# Recent-window cap (years) for the heavy DAILY marts so the dashboard files and
# the pod's JSON parse stay small. The marts hold ~26y of daily history; the UI
# only needs the recent tail for detail charts + the 52w range.
EQUITY_WINDOW_YEARS = 2

# Names / sectors for the B3 cash-market watchlist (no dim_security in silver yet).
B3_NAMES = {
    "PETR4": ("Petrobras PN", "Oil & Gas"), "PETR3": ("Petrobras ON", "Oil & Gas"),
    "VALE3": ("Vale", "Materials"), "ITUB4": ("Itaú Unibanco", "Financials"),
    "BBDC4": ("Bradesco", "Financials"), "BBAS3": ("Banco do Brasil", "Financials"),
    "B3SA3": ("B3", "Financials"), "ITSA4": ("Itaúsa", "Financials"),
    "ABEV3": ("Ambev", "Consumer Staples"), "WEGE3": ("WEG", "Industrials"),
    "RENT3": ("Localiza", "Industrials"), "PRIO3": ("PetroRio", "Oil & Gas"),
    "SUZB3": ("Suzano", "Materials"), "GGBR4": ("Gerdau", "Materials"),
    "MGLU3": ("Magazine Luiza", "Consumer Disc."),
}


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL delta; LOAD delta;")
    con.execute(f"""
        CREATE OR REPLACE SECRET minio (
            TYPE S3,
            KEY_ID  '{USER}',
            SECRET  '{PASSWORD}',
            ENDPOINT '{host_port}',
            URL_STYLE 'path',
            USE_SSL  {use_ssl},
            REGION   'us-east-1'
        )
    """)
    return con


def _jsonable(v):
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()
    return v


def _write(name: str, records: list[dict]):
    out = os.path.join(OUT, f"{name}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(records, f, separators=(",", ":"))
    print(f"  {name:24s} {len(records):>6d} rows")


def _query(con, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [{c: _jsonable(v) for c, v in zip(cols, row)} for row in cur.fetchall()]


def dump(con, name: str, path: str):
    # duckdb-only (no pandas) — keeps the cluster CronJob to a single `pip install duckdb`
    _write(name, _query(con, f"SELECT * FROM delta_scan('s3://{BUCKET}/{path}')"))


def dump_fact_observation(con):
    """silver/fact_observation, downsampled to month-end so the catalog stays light."""
    sql = f"""
        SELECT series_id,
               CAST(date_trunc('month', date) AS DATE) AS date,
               arg_max(value, date)  AS value,
               any_value(source)     AS source
        FROM delta_scan('s3://{BUCKET}/silver/fact_observation')
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    _write("fact_observation", _query(con, sql))


def _downsample(xs: list, n: int = 40) -> list:
    if len(xs) <= n:
        return [round(float(x), 2) for x in xs]
    step = (len(xs) - 1) / (n - 1)
    return [round(float(xs[round(i * step)]), 2) for i in range(n)]


def dump_equity_daily_windowed(con):
    """gold/mart_equity_daily, capped to the recent window (heavy: ~26y daily)."""
    sql = f"""
        SELECT * FROM delta_scan('s3://{BUCKET}/gold/mart_equity_daily')
        WHERE date >= (current_date - INTERVAL {EQUITY_WINDOW_YEARS} YEAR)
    """
    _write("mart_equity_daily", _query(con, sql))


def dump_yield_curve_snapshots(con):
    """gold/mart_yield_curve, thinned to one month-end snapshot per bond (heavy: daily)."""
    sql = f"""
        SELECT * FROM delta_scan('s3://{BUCKET}/gold/mart_yield_curve')
        QUALIFY row_number() OVER (
            PARTITION BY bond, date_trunc('month', date) ORDER BY date DESC
        ) = 1
        ORDER BY date, bond
    """
    _write("mart_yield_curve", _query(con, sql))


# Curated underlyings for the derivatives page (the rest are single-stock options).
CURVE_ASSETS = ["DI1", "DAP", "DDI"]                       # clean term structures
OI_ASSETS = ["DI1", "DOL", "WDO", "IND", "WIN", "BGI", "CCM", "ETH", "ICF", "DAP"]


def dump_futures_curve(con):
    """gold/mart_futures_curve (1.6M rows) → month-end snapshots of the DI-family
    term structures only, so the dashboard can draw settlement_rate vs days_to_maturity."""
    assets = ",".join(f"'{a}'" for a in CURVE_ASSETS)
    sql = f"""
        SELECT date, asset, symbol, days_to_maturity, maturity,
               settlement_rate, settlement_price, open_interest
        FROM delta_scan('s3://{BUCKET}/gold/mart_futures_curve')
        WHERE asset IN ({assets})
          AND settlement_rate IS NOT NULL
          AND date >= (current_date - INTERVAL 18 MONTH)
        QUALIFY row_number() OVER (
            PARTITION BY asset, symbol, date_trunc('month', date) ORDER BY date DESC
        ) = 1
        ORDER BY date, asset, days_to_maturity
    """
    _write("mart_futures_curve", _query(con, sql))


def dump_open_interest(con):
    """gold/mart_open_interest → curated macro underlyings, full daily series."""
    assets = ",".join(f"'{a}'" for a in OI_ASSETS)
    sql = f"""
        SELECT date, asset, segment, total_open_interest, total_open_interest_var, n_contracts
        FROM delta_scan('s3://{BUCKET}/gold/mart_open_interest')
        WHERE asset IN ({assets})
        ORDER BY asset, date
    """
    _write("mart_open_interest", _query(con, sql))


def dump_equity_universe(con):
    """Derive the whole B3 cash-market snapshot from gold/mart_equity_daily.
    B3 round-lot tickers are plain (PETR4, VALE3, B3SA3) — no .SA / ^ / = suffix."""
    sql = f"""
        SELECT symbol                              AS symbol,
               arg_max(close, date)                AS close,
               arg_max(daily_return_pct, date)     AS daily_return_pct,
               arg_max(vol_21d, date)              AS vol_21d,
               max(high_52w)                       AS high_52w,
               min(low_52w)                        AS low_52w,
               list(close ORDER BY date)           AS closes
        FROM delta_scan('s3://{BUCKET}/gold/mart_equity_daily')
        WHERE regexp_matches(symbol, '^[A-Z0-9]{{4}}[0-9]{{1,2}}$')
          AND date >= (current_date - INTERVAL {EQUITY_WINDOW_YEARS} YEAR)
        GROUP BY symbol
    """
    rows = _query(con, sql)
    for row in rows:
        name, sector = B3_NAMES.get(row["symbol"], (row["symbol"], "Equity"))
        row["name"], row["sector"] = name, sector
        row["spark"] = _downsample(row.pop("closes") or [])
    rows.sort(key=lambda r: r["name"])
    _write("mart_equity_universe", rows)


def main():
    print(f"exporting live snapshot from {ENDPOINT} (bucket {BUCKET}) -> {OUT}")
    con = connect()
    for mart in MARTS:
        dump(con, mart, f"gold/{mart}")
    dump(con, "dim_series", "silver/dim_series")
    # heavy daily marts -> windowed / thinned; extra tables derived.
    # best-effort: don't fail the whole export if one table is absent yet.
    for label, fn in (
        ("mart_equity_daily", dump_equity_daily_windowed),
        ("mart_yield_curve", dump_yield_curve_snapshots),
        ("fact_observation", dump_fact_observation),
        ("mart_equity_universe", dump_equity_universe),
        ("mart_futures_curve", dump_futures_curve),
        ("mart_open_interest", dump_open_interest),
    ):
        try:
            fn(con)
        except Exception as exc:  # noqa: BLE001
            print(f"  ! skipped {label}: {exc}")
    with open(os.path.join(OUT, "_meta.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_from": "live", "endpoint": ENDPOINT, "bucket": BUCKET}, f, indent=2)
    print("done.")


if __name__ == "__main__":
    main()
