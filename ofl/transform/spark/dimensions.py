"""Conformed dimensions for the silver star schema.

``dim_series`` is generated directly from the registry (no I/O) — the registry is
the business metadata. ``dim_date`` is a generated calendar covering the data span.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ofl.platform.io import silver_uri, to_spark_path
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
