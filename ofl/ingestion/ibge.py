"""IBGE extractor (Polars) — real ``servicodados`` agregados API, ``observation`` fact.

Used for indicators IBGE owns that BACEN/SGS does not duplicate (e.g. the PNAD
Contínua unemployment rate). The API nests values under resultados->series->serie
keyed by ``YYYYMM`` periods.

A series names a table (``agregado``) and a variable in it (``indicador``). The older
``/api/v1/indicadores/{id}`` route answered 503 after 60 s for every caller in 2026-10,
so the handler reads the table route, which is what IBGE documents. Pick a MONTHLY
table: quarterly ones key periods as ``YYYYQQ``, which would be read as months.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import requests

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

IBGE_URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}"
    "/periodos/all/variaveis/{indicador}?localidades=N1[all]"
)
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


def fetch_ibge(agregado: int, indicador: int) -> pl.DataFrame:
    resp = requests.get(IBGE_URL.format(agregado=agregado, indicador=indicador), timeout=60)
    resp.raise_for_status()
    return _normalize(resp.json())


def ingest_ibge(series: Series) -> dict:
    agregado = series.extra.get("agregado")
    indicador = series.extra.get("indicador")
    if not agregado or not indicador:
        raise ValueError(f"series '{series.key}' has handler ibge but needs extra.agregado + extra.indicador")
    df = fetch_ibge(int(agregado), int(indicador))
    log.info("ibge_fetched", series=series.key, agregado=agregado, indicador=indicador, rows=df.height)
    return land_bronze(series, df)
