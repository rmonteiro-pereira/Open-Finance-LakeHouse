"""Ingestion lane (Polars). Dispatches a registered series to its handler and
lands it in the bronze Delta layer.

Adding a new source family = implement a handler ``ingest_<x>(series) -> dict``
and register it in ``_HANDLERS``.
"""

from __future__ import annotations

from ofl.ingestion.bacen import ingest_bacen_sgs
from ofl.ingestion.ibge import ingest_ibge
from ofl.ingestion.tesouro import ingest_tesouro
from ofl.ingestion.yahoo import ingest_yahoo
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)

_HANDLERS = {
    "bacen_sgs": ingest_bacen_sgs,
    "yahoo": ingest_yahoo,
    "tesouro_direto": ingest_tesouro,
    "ibge": ingest_ibge,
    # planned (legacy impls were mock/synthetic — need real API integration):
    #   "ipea", "b3", "anbima"
}


def run_ingestion(series: Series) -> dict:
    handler = _HANDLERS.get(series.handler)
    if handler is None:
        log.warning("handler_not_implemented", series=series.key, handler=series.handler)
        return {"series": series.key, "skipped": True, "reason": "handler_not_implemented"}
    return handler(series)
