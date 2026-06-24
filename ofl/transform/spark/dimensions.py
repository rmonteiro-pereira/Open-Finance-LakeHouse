"""Conformed dimensions for the silver star schema.

``dim_series`` is generated directly from the registry (no I/O) — the registry is
the business metadata. ``dim_date`` is a generated calendar covering the data span.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ofl.platform.io import bronze_uri, silver_uri, to_spark_path
from ofl.platform.logging import get_logger
from ofl.registry import Registry, load_registry

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger(__name__)


def build_dim_series(spark: "SparkSession", registry: Registry | None = None) -> str:
    """Materialize ``silver.dim_series`` from the registry (overwrite — it's metadata)."""
    reg = registry or load_registry()
    rows = [
        {
            "series_id": s.key,
            "name": s.name,
            "domain": s.domain,
            "source": s.handler,
            "category": s.category,
            "unit": s.unit,
            "frequency": s.frequency,
            "fact": s.fact,
        }
        for s in reg.series.values()
    ]
    uri = to_spark_path(silver_uri("dim_series"))
    (
        spark.createDataFrame(rows)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(uri)
    )
    log.info("dim_series_built", rows=len(rows), uri=uri)
    return uri


def build_dim_instrument(spark: "SparkSession", registry: Registry | None = None) -> str | None:
    """Materialize ``silver.dim_instrument`` from the latest B3 instrument snapshot.

    The ``fact: instrument`` bronze tables are daily full-universe snapshots; the
    dimension keeps the most recent row per symbol (by snapshot date, then ingest
    time). Overwrite — it's a current-state dimension, not a history.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.window import Window

    reg = registry or load_registry()
    frames = []
    for s in reg.active():
        if s.fact != "instrument":
            continue
        uri = to_spark_path(bronze_uri("instrument", s.key))
        try:
            frames.append(spark.read.format("delta").load(uri))
        except AnalysisException:
            log.warning("bronze_missing", series=s.key, uri=uri)

    if not frames:
        log.warning("no_bronze_instruments")
        return None

    unioned = frames[0]
    for f in frames[1:]:
        unioned = unioned.unionByName(f, allowMissingColumns=True)

    latest = Window.partitionBy("symbol").orderBy(F.col("date").desc(), F.col("ingested_at").desc())
    dim = (
        unioned.withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "series_id", "domain", "load_id")
    )

    uri = to_spark_path(silver_uri("dim_instrument"))
    dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(uri)
    log.info("dim_instrument_built", rows=dim.count(), uri=uri)
    return uri


def build_dim_date(spark: "SparkSession", start: str = "1980-01-01", end: str = "2035-12-31") -> str:
    """Materialize ``silver.dim_date`` calendar over the data span."""
    from pyspark.sql import functions as F

    uri = to_spark_path(silver_uri("dim_date"))
    df = (
        spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("is_month_end", F.expr("date = last_day(date)"))
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(uri)
    log.info("dim_date_built", uri=uri)
    return uri
