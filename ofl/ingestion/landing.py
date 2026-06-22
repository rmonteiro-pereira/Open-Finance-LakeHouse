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


def _last_obs_epoch(df: pl.DataFrame) -> float | None:
    """Latest observation *data* date as UTC epoch seconds (for freshness alerts).

    Freshness is asserted on the data date, not the run wall-clock, so weekends,
    holidays and monthly quiet periods on slow series don't read as "stale".
    """
    if "date" not in df.columns:
        return None
    d = df.select(pl.col("date").max()).item()
    if d is None:
        return None
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()


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

    # Quality gate: enforce the contract before anything reaches storage. A breach
    # raises a per-series DQ metric (-> individual Alertmanager e-mail) and then
    # re-raises so the task FAILS and this series' bronze asset is withheld — bad
    # data never propagates to silver, and the other series are untouched.
    if series.fact == "observation":
        from ofl.quality.contracts import validate_observation

        try:
            validate_observation(enriched, max_value=series.max_value)
        except Exception as exc:  # noqa: BLE001 - any contract breach is a DQ failure
            from ofl.platform.metrics import record_dq_failure

            record_dq_failure(series, check=type(exc).__name__)
            log.warning("dq_violation", series=series.key, check=type(exc).__name__, error=str(exc))
            raise

    uri = bronze_uri(series.fact, series.key)
    write_deltalake(
        uri,
        enriched.to_arrow(),
        mode=mode,
        storage_options=delta_storage_options(),
    )
    log.info("bronze_landed", series=series.key, rows=enriched.height, uri=uri, load_id=load_id)

    # Healthy run: refresh per-series freshness (data date) and clear gauges.
    from ofl.platform.metrics import record_ingest_success

    record_ingest_success(series, last_obs_epoch=_last_obs_epoch(enriched))
    return {"series": series.key, "rows": enriched.height, "load_id": load_id, "uri": uri}
