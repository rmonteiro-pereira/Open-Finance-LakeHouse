"""Bronze landing — write an extracted Polars frame to Delta with load metadata.

Bronze keeps raw-but-typed data, one Delta table per series under
``bronze/{fact}/{series_key}``. The silver lane (Spark) later unions these into
the conformed ``fact_observation`` and friends.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import polars as pl

from ofl.platform.io import bronze_uri, delta_storage_options
from ofl.platform.logging import get_logger
from ofl.registry import Series

log = get_logger(__name__)


def land_bronze(series: Series, df: pl.DataFrame, *, mode: str = "overwrite") -> dict:
    """Land ``df`` to the bronze Delta table for ``series`` with lineage columns.

    ``mode='overwrite'`` is safe here because the SGS extractor returns the full
    series each run (small data); switch to ``append`` + watermark for sources
    that support incremental pulls.
    """
    from deltalake import write_deltalake  # heavy; only needed when actually writing

    load_id = uuid.uuid4().hex
    ingested_at = datetime.now(timezone.utc)

    enriched = df.with_columns(
        pl.lit(series.key).alias("series_id"),
        pl.lit(series.domain).alias("domain"),
        pl.lit(series.handler).alias("source"),
        pl.lit(ingested_at).alias("ingested_at"),
        pl.lit(load_id).alias("load_id"),
    )

    # Quality gate: enforce the contract before anything reaches storage.
    if series.fact == "observation":
        from ofl.quality.contracts import validate_observation

        validate_observation(enriched, max_value=series.max_value)

    uri = bronze_uri(series.fact, series.key)
    write_deltalake(
        uri,
        enriched.to_arrow(),
        mode=mode,
        storage_options=delta_storage_options(),
    )
    log.info("bronze_landed", series=series.key, rows=enriched.height, uri=uri, load_id=load_id)
    return {"series": series.key, "rows": enriched.height, "load_id": load_id, "uri": uri}
