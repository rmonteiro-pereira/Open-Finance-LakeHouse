"""BACEN Focus / Expectativas de Mercado extractor (Polars).

A second BACEN lane: the weekly market-expectations survey (Focus), served by
the open Olinda OData API (no auth). Each registered ``bacen_focus`` series pins
one indicator + horizon and lands a tidy ``(date, value)`` frame of the survey
*median*, conforming to the same ``fact_observation`` star as the realized SGS
series. This unlocks *ex-ante* reads — e.g. real interest = Selic − expected IPCA.

Two horizon shapes are supported via ``extra``:
- rolling (default): a resource that is already a single forward-looking series
  (``ExpectativasMercadoInflacao12Meses`` / ``...24Meses``) — taken as-is.
- ``horizon: current_year``: an annual resource (``ExpectativasMercadoAnuais``)
  carrying every reference year per survey date; we keep only the row whose
  ``DataReferencia`` equals the survey-date year, collapsing it to one series
  (the "end of current year" expectation).
"""

from __future__ import annotations

from urllib.parse import quote, urlencode

import polars as pl
import requests

from ofl.ingestion.landing import land_bronze
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

FOCUS_BASE = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata"
_PAGE = 10000  # Olinda caps an OData page at 10k rows


def _odata_filter(extra: dict) -> str:
    """Build the ``$filter`` clause pinning one indicator (and survey base)."""
    parts = [f"Indicador eq '{extra['indicador']}'"]
    if extra.get("suavizada"):
        parts.append(f"Suavizada eq '{extra['suavizada']}'")
    if extra.get("base_calculo") is not None:
        parts.append(f"baseCalculo eq {int(extra['base_calculo'])}")
    return " and ".join(parts)


def fetch_focus(extra: dict) -> list[dict]:
    """Page through one Focus OData resource for a single indicator.

    ``requests`` percent-encodes the OData params (so non-ASCII indicators like
    ``Câmbio`` are handled). Pages in 10k blocks until a short page ends it.
    """
    url = f"{FOCUS_BASE}/{extra['resource']}"
    rows: list[dict] = []
    skip = 0
    while True:
        params = {
            "$format": "json",
            "$orderby": "Data",
            "$top": str(_PAGE),
            "$skip": str(skip),
            "$filter": _odata_filter(extra),
        }
        # Olinda rejects '+'-encoded spaces in $filter — force %20 via quote_via.
        query = urlencode(params, quote_via=quote)
        resp = requests.get(f"{url}?{query}", timeout=90)
        resp.raise_for_status()
        batch = resp.json().get("value", [])
        rows.extend(batch)
        if len(batch) < _PAGE:
            return rows
        skip += _PAGE


def ingest_bacen_focus(series: Series) -> dict:
    e = series.extra
    if not e.get("resource") or not e.get("indicador"):
        raise ValueError(
            f"series '{series.key}' has handler bacen_focus but needs extra.resource + extra.indicador"
        )

    rows = fetch_focus(e)
    if not rows:
        log.warning("focus_empty", series=series.key, resource=e["resource"])
        return land_bronze(series, pl.DataFrame(schema={"date": pl.Date, "value": pl.Float64}))

    value_field = e.get("value_field", "Mediana")
    horizon = e.get("horizon")
    # Build only the columns we need (the raw OData rows carry nulls/mixed types
    # like IndicadorDetalhe that trip Polars' dict schema inference).
    data: dict[str, list] = {
        "date": [r.get("Data") for r in rows],
        "value": [r.get(value_field) for r in rows],
    }
    if horizon == "current_year":
        data["_ref"] = [str(r.get("DataReferencia")) for r in rows]

    df = pl.DataFrame(data).select(
        pl.col("date").str.to_date("%Y-%m-%d"),
        pl.col("value").cast(pl.Float64, strict=False),
        *([pl.col("_ref")] if horizon == "current_year" else []),
    )
    if horizon == "current_year":
        df = df.filter(pl.col("_ref") == pl.col("date").dt.year().cast(pl.Utf8)).drop("_ref")

    df = df.drop_nulls(["date", "value"]).unique(subset="date", keep="last").sort("date")
    log.info("focus_fetched", series=series.key, resource=e["resource"], rows=df.height)
    return land_bronze(series, df)
