"""IBGE extractor (Polars) — real ``servicodados`` indicadores API, ``observation`` fact.

Used for indicators IBGE owns that BACEN/SGS does not duplicate (e.g. the PNAD
Contínua unemployment rate). The API nests values under resultados->series->serie
keyed by ``YYYYMM`` periods.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import requests

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/indicadores/{indicador}"
_SKIP = {None, "...", "-", ""}


def _normalize(payload: list) -> pl.DataFrame:
    """Flatten the nested IBGE indicador payload to ``(date, value)``."""
    rows: list[tuple[date, float]] = []
    for record in payload:
        for resultado in record.get("resultados", []):
            for serie in resultado.get("series", []):
                for period, value in serie.get("serie", {}).items():
                    if value in _SKIP or len(period) != 6:
                        continue
                    try:
                        d = date(int(period[:4]), int(period[4:]), 1)
                        rows.append((d, float(str(value).replace(",", "."))))
                    except (ValueError, TypeError):
                        continue
    if not rows:
        return pl.DataFrame(schema={"date": pl.Date, "value": pl.Float64})
    return (
        pl.DataFrame(rows, schema={"date": pl.Date, "value": pl.Float64}, orient="row")
        .unique("date", keep="last")
        .sort("date")
    )


def fetch_ibge(indicador: int) -> pl.DataFrame:
    resp = requests.get(IBGE_URL.format(indicador=indicador), timeout=60)
    resp.raise_for_status()
    return _normalize(resp.json())


def ingest_ibge(series: Series) -> dict:
    indicador = series.extra.get("indicador")
    if not indicador:
        raise ValueError(f"series '{series.key}' has handler ibge but no indicador")
    df = fetch_ibge(int(indicador))
    log.info("ibge_fetched", series=series.key, indicador=indicador, rows=df.height)
    return land_bronze(series, df)
