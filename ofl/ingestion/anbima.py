"""ANBIMA Feed extractor — real OAuth2 API, ``treasury`` fact.

ANBIMA's pricing APIs require **registered developer credentials** (no free
anonymous access). Register at developers.anbima.com.br, then store the
credentials in OpenBao at ``secret/claude/anbima`` (fields ``client_id`` /
``client_secret``) and inject them as env (``ANBIMA_CLIENT_ID`` /
``ANBIMA_CLIENT_SECRET``) — the same pattern as the MinIO creds.

Flow: Basic-auth -> bearer ``access_token`` (1h) -> secondary-market TPF prices.
Until credentials exist, this series stays ``status: planned`` in the registry.
"""

from __future__ import annotations

import base64
import os

import polars as pl
import requests

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

TOKEN_URL = "https://api.anbima.com.br/oauth/access-token"
TPF_URL = "https://api.anbima.com.br/feed/precos-indices/v1/titulos-publicos/mercado-secundario-TPF"

# bronze treasury schema (matches silver.fact_treasury inputs)
_STR_SCHEMA = {
    "bond": pl.String, "maturity": pl.String, "date": pl.String,
    "buy_rate": pl.Float64, "sell_rate": pl.Float64,
    "buy_price": pl.Float64, "sell_price": pl.Float64,
}


def _num(v) -> float | None:
    if v in (None, ""):
        return None
    if isinstance(v, str):
        v = v.replace(",", ".")
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse(records: list) -> pl.DataFrame:
    """Map ANBIMA TPF records to the canonical treasury frame.

    ``sell_rate`` carries ``taxa_indicativa`` (the reference yield used for the
    curve); ``buy_rate`` carries the bid (``taxa_compra``); ``pu`` is the price.
    """
    rows = [
        {
            "bond": r.get("codigo_selic"),
            "maturity": str(r.get("data_vencimento") or "")[:10],
            "date": str(r.get("data_referencia") or "")[:10],
            "buy_rate": _num(r.get("taxa_compra")),
            "sell_rate": _num(r.get("taxa_indicativa")),
            "buy_price": _num(r.get("pu")),
            "sell_price": _num(r.get("pu")),
        }
        for r in records
    ]
    if not rows:
        return pl.DataFrame(schema={**_STR_SCHEMA, "maturity": pl.Date, "date": pl.Date})
    return (
        pl.DataFrame(rows, schema=_STR_SCHEMA)
        .with_columns(
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("maturity").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        )
        .drop_nulls("date")
        .sort("bond", "date")
    )


def _credentials() -> tuple[str, str]:
    cid, secret = os.getenv("ANBIMA_CLIENT_ID"), os.getenv("ANBIMA_CLIENT_SECRET")
    if not cid or not secret:
        raise RuntimeError(
            "ANBIMA credentials missing — set ANBIMA_CLIENT_ID / ANBIMA_CLIENT_SECRET "
            "(store in OpenBao secret/claude/anbima and inject as env)."
        )
    return cid, secret


def _access_token(client_id: str, client_secret: str) -> str:
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = requests.post(
        TOKEN_URL,
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/json"},
        json={"grant_type": "client_credentials"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_anbima_tpf(client_id: str, token: str, *, data: str | None = None) -> pl.DataFrame:
    params = {"data": data} if data else {}
    resp = requests.get(
        TPF_URL,
        headers={"client_id": client_id, "access_token": token},
        params=params,
        timeout=60,
    )
    resp.raise_for_status()
    return _parse(resp.json())


def ingest_anbima(series: Series) -> dict:
    client_id, client_secret = _credentials()
    token = _access_token(client_id, client_secret)
    df = fetch_anbima_tpf(client_id, token)
    log.info("anbima_fetched", series=series.key, rows=df.height)
    return land_bronze(series, df, mode="append")  # daily snapshot; silver MERGE dedups
