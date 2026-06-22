"""Tesouro Direto extractor (Polars) — real CKAN open-data feed, ``treasury`` fact.

Downloads the official "Preços e Taxas dos Títulos" CSV from Tesouro Transparente
(semicolon-separated, Brazilian comma decimals) and lands a long per-bond frame.
"""

from __future__ import annotations

import io

import polars as pl
import requests

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

CKAN = "https://www.tesourotransparente.gov.br/ckan/api/3/action"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ofl-lakehouse/1.0)"}

# Source CSV column -> canonical name.
_COLS = {
    "Tipo Titulo": "bond",
    "Data Vencimento": "maturity",
    "Data Base": "date",
    "Taxa Compra Manha": "buy_rate",
    "Taxa Venda Manha": "sell_rate",
    "PU Compra Manha": "buy_price",
    "PU Venda Manha": "sell_price",
}
_NUMERIC = ["buy_rate", "sell_rate", "buy_price", "sell_price"]


def _to_long(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize the raw (all-string) CSV frame to typed long form."""
    present = {src: dst for src, dst in _COLS.items() if src in df.columns}
    out = df.select(list(present)).rename(present)
    return (
        out.with_columns(
            pl.col("date").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            pl.col("maturity").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            *[
                # Brazilian format: strip thousand-separator dots, then comma -> dot.
                pl.col(c).str.replace_all(".", "", literal=True).str.replace(",", ".").cast(pl.Float64, strict=False)
                for c in _NUMERIC
                if c in present.values()
            ],
        )
        .drop_nulls("date")
        .sort("bond", "date")
    )


def _resource_url(dataset_id: str) -> str:
    resp = requests.get(f"{CKAN}/package_show", params={"id": dataset_id}, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resources = resp.json()["result"]["resources"]
    csvs = [r for r in resources if str(r.get("format", "")).upper() == "CSV"]
    if not csvs:
        raise RuntimeError("no CSV resource found in the Tesouro dataset")
    return max(csvs, key=lambda r: r.get("created", ""))["url"]


def fetch_tesouro(dataset_id: str) -> pl.DataFrame:
    content = requests.get(_resource_url(dataset_id), headers=HEADERS, timeout=180).content
    raw = pl.read_csv(io.BytesIO(content), separator=";", infer_schema_length=0)  # all Utf8
    return _to_long(raw)


def ingest_tesouro(series: Series) -> dict:
    dataset_id = series.extra.get("dataset_id")
    if not dataset_id:
        raise ValueError(f"series '{series.key}' has handler tesouro_direto but no dataset_id")
    df = fetch_tesouro(dataset_id)
    log.info("tesouro_fetched", series=series.key, rows=df.height, bonds=df["bond"].n_unique())
    return land_bronze(series, df)
