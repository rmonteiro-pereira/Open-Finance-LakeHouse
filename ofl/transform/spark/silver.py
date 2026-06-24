"""Bronze -> Silver conform for single-value macro series.

Unions every ``fact: observation`` bronze table into the conformed long fact
``silver.fact_observation`` via an **idempotent Delta MERGE** (safe to re-run),
then derives ``silver.series_metrics`` with Spark **window functions**.

Runs on the cluster (Spark + Delta + S3A). Tiny data, so partitioning/ZORDER are
here as correct, demonstrable technique rather than a scale necessity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ofl.platform.io import bronze_uri, silver_uri, to_spark_path
from ofl.platform.logging import get_logger
from ofl.registry import Registry, load_registry

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

# Canonical silver schema for the conformed observation fact.
_FACT_OBSERVATION_DDL = (
    "series_id STRING, date DATE, value DOUBLE, "
    "source STRING, ingested_at TIMESTAMP, load_id STRING"
)


def _read_bronze_observations(spark: "SparkSession", reg: Registry) -> "DataFrame | None":
    """Union all available bronze observation tables into the canonical schema."""
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    frames = []
    for s in reg.active():
        if s.fact != "observation":
            continue
        uri = to_spark_path(bronze_uri("observation", s.key))
        try:
            df = spark.read.format("delta").load(uri)
        except AnalysisException:
            log.warning("bronze_missing", series=s.key, uri=uri)
            continue
        frames.append(
            df.select(
                F.col("series_id").cast("string"),
                F.col("date").cast("date"),
                F.col("value").cast("double"),
                F.col("source").cast("string"),
                F.col("ingested_at").cast("timestamp"),
                F.col("load_id").cast("string"),
            )
        )

    if not frames:
        return None

    unioned = frames[0]
    for f in frames[1:]:
        unioned = unioned.unionByName(f)

    # Enforce one row per (series_id, date): keep the latest ingestion.
    from pyspark.sql.window import Window

    dedup = Window.partitionBy("series_id", "date").orderBy(F.col("ingested_at").desc())
    return (
        unioned.withColumn("_rn", F.row_number().over(dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def conform_observations(spark: "SparkSession", registry: Registry | None = None) -> dict:
    """MERGE all bronze observation series into ``silver.fact_observation``."""
    from delta.tables import DeltaTable

    reg = registry or load_registry()
    source = _read_bronze_observations(spark, reg)
    if source is None:
        log.warning("no_bronze_observations")
        return {"merged": 0}

    uri = to_spark_path(silver_uri("fact_observation"))
    (
        DeltaTable.createIfNotExists(spark)
        .location(uri)
        .addColumns(spark.createDataFrame([], _FACT_OBSERVATION_DDL).schema)
        .partitionedBy("source")
        .execute()
    )

    target = DeltaTable.forPath(spark, uri)
    (
        target.alias("t")
        .merge(source.alias("s"), "t.series_id = s.series_id AND t.date = s.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    count = spark.read.format("delta").load(uri).count()
    log.info("fact_observation_merged", total_rows=count, uri=uri)
    return {"merged": count, "uri": uri}


_FACT_TREASURY_DDL = (
    "bond STRING, maturity DATE, date DATE, "
    "buy_rate DOUBLE, sell_rate DOUBLE, buy_price DOUBLE, sell_price DOUBLE, "
    "source STRING, ingested_at TIMESTAMP, load_id STRING"
)


def conform_treasury(spark: "SparkSession", registry: Registry | None = None) -> dict:
    """MERGE bronze treasury series into ``silver.fact_treasury`` (grain bond x date)."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.window import Window

    reg = registry or load_registry()
    frames = []
    for s in reg.active():
        if s.fact != "treasury":
            continue
        uri = to_spark_path(bronze_uri("treasury", s.key))
        try:
            frames.append(spark.read.format("delta").load(uri))
        except AnalysisException:
            log.warning("bronze_missing", series=s.key, uri=uri)

    if not frames:
        log.warning("no_bronze_treasury")
        return {"merged": 0}

    unioned = frames[0]
    for f in frames[1:]:
        unioned = unioned.unionByName(f)

    dedup = Window.partitionBy("bond", "date").orderBy(F.col("ingested_at").desc())
    source = (
        unioned.withColumn("_rn", F.row_number().over(dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "series_id", "domain")
    )

    uri = to_spark_path(silver_uri("fact_treasury"))
    (
        DeltaTable.createIfNotExists(spark)
        .location(uri)
        .addColumns(spark.createDataFrame([], _FACT_TREASURY_DDL).schema)
        .execute()
    )
    target = DeltaTable.forPath(spark, uri)
    (
        target.alias("t")
        .merge(source.alias("s"), "t.bond = s.bond AND t.date = s.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    count = spark.read.format("delta").load(uri).count()
    log.info("fact_treasury_merged", total_rows=count, uri=uri)
    return {"merged": count, "uri": uri}


_FACT_SECURITY_PRICE_DDL = (
    "symbol STRING, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, "
    "volume DOUBLE, source STRING, ingested_at TIMESTAMP, load_id STRING"
)
_OHLCV = ["open", "high", "low", "close", "volume"]


def conform_security_prices(spark: "SparkSession", registry: Registry | None = None) -> dict:
    """MERGE bronze security_price series into ``silver.fact_security_price``.

    Unions every ``fact: security_price`` source (Yahoo families, b3 index,
    b3_cotahist) on the common OHLCV schema (grain: symbol x date). Sources carry
    different extra columns (cotahist adds volume_brl/trades/fatcot; some Yahoo
    index symbols lack volume), so we union allowing missing columns and project
    the canonical OHLCV.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.window import Window

    reg = registry or load_registry()
    frames = []
    for s in reg.active():
        if s.fact != "security_price":
            continue
        uri = to_spark_path(bronze_uri("security_price", s.key))
        try:
            frames.append(spark.read.format("delta").load(uri))
        except AnalysisException:
            log.warning("bronze_missing", series=s.key, uri=uri)

    if not frames:
        log.warning("no_bronze_security_prices")
        return {"merged": 0}

    unioned = frames[0]
    for f in frames[1:]:
        unioned = unioned.unionByName(f, allowMissingColumns=True)

    for c in _OHLCV:  # a source may not carry every OHLCV column (e.g. index symbols)
        if c not in unioned.columns:
            unioned = unioned.withColumn(c, F.lit(None))

    dedup = Window.partitionBy("symbol", "date").orderBy(F.col("ingested_at").desc())
    source = (
        unioned.withColumn("_rn", F.row_number().over(dedup))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("symbol").cast("string"),
            F.col("date").cast("date"),
            *[F.col(c).cast("double") for c in _OHLCV],
            F.col("source").cast("string"),
            F.col("ingested_at").cast("timestamp"),
            F.col("load_id").cast("string"),
        )
    )

    uri = to_spark_path(silver_uri("fact_security_price"))
    (
        DeltaTable.createIfNotExists(spark)
        .location(uri)
        .addColumns(spark.createDataFrame([], _FACT_SECURITY_PRICE_DDL).schema)
        .partitionedBy("source")
        .execute()
    )
    target = DeltaTable.forPath(spark, uri)
    (
        target.alias("t")
        .merge(source.alias("s"), "t.symbol = s.symbol AND t.date = s.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    count = spark.read.format("delta").load(uri).count()
    log.info("fact_security_price_merged", total_rows=count, uri=uri)
    return {"merged": count, "uri": uri}


_FACT_OPEN_INTEREST_DDL = (
    "symbol STRING, date DATE, asset STRING, expiration_code STRING, segment STRING, "
    "isin STRING, open_interest DOUBLE, open_interest_var DOUBLE, "
    "source STRING, ingested_at TIMESTAMP, load_id STRING"
)
_OI_COLS = ["symbol", "date", "asset", "expiration_code", "segment", "isin", "open_interest", "open_interest_var"]

_FACT_DERIV_QUOTE_DDL = (
    "symbol STRING, date DATE, segment STRING, isin STRING, "
    "min_price DOUBLE, max_price DOUBLE, avg_price DOUBLE, last_price DOUBLE, oscillation_pct DOUBLE, "
    "settlement_price DOUBLE, settlement_rate DOUBLE, ref_price DOUBLE, "
    "trades DOUBLE, contracts DOUBLE, notional DOUBLE, "
    "source STRING, ingested_at TIMESTAMP, load_id STRING"
)
_DQ_COLS = [
    "symbol", "date", "segment", "isin", "min_price", "max_price", "avg_price", "last_price",
    "oscillation_pct", "settlement_price", "settlement_rate", "ref_price", "trades", "contracts", "notional",
]


def _conform_b3_fact(
    spark: "SparkSession", reg: Registry, *, fact: str, table: str, cols: list[str], ddl: str
) -> dict:
    """Generic MERGE of a B3-portal fact (grain symbol x date) into silver.

    Both ``fact_open_interest`` and ``fact_derivatives_quote`` union their bronze
    tables, keep the latest ingestion per (symbol, date), and upsert. Sources share
    one schema per fact, so a plain ``unionByName`` (no missing-column fill) holds.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.window import Window

    frames = []
    for s in reg.active():
        if s.fact != fact:
            continue
        uri = to_spark_path(bronze_uri(fact, s.key))
        try:
            frames.append(spark.read.format("delta").load(uri))
        except AnalysisException:
            log.warning("bronze_missing", series=s.key, uri=uri)

    if not frames:
        log.warning("no_bronze", fact=fact)
        return {"merged": 0}

    unioned = frames[0]
    for f in frames[1:]:
        unioned = unioned.unionByName(f, allowMissingColumns=True)

    dedup = Window.partitionBy("symbol", "date").orderBy(F.col("ingested_at").desc())
    source = (
        unioned.withColumn("_rn", F.row_number().over(dedup))
        .filter(F.col("_rn") == 1)
        .select(*cols, F.col("source"), F.col("ingested_at"), F.col("load_id"))
    )

    uri = to_spark_path(silver_uri(table))
    (
        DeltaTable.createIfNotExists(spark)
        .location(uri)
        .addColumns(spark.createDataFrame([], ddl).schema)
        .partitionedBy("source")
        .execute()
    )
    target = DeltaTable.forPath(spark, uri)
    (
        target.alias("t")
        .merge(source.alias("s"), "t.symbol = s.symbol AND t.date = s.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    count = spark.read.format("delta").load(uri).count()
    log.info("b3_fact_merged", table=table, total_rows=count, uri=uri)
    return {"merged": count, "uri": uri}


def conform_open_interest(spark: "SparkSession", registry: Registry | None = None) -> dict:
    """MERGE bronze ``open_interest`` series into ``silver.fact_open_interest``."""
    reg = registry or load_registry()
    return _conform_b3_fact(
        spark, reg, fact="open_interest", table="fact_open_interest", cols=_OI_COLS, ddl=_FACT_OPEN_INTEREST_DDL
    )


def conform_derivatives_quotes(spark: "SparkSession", registry: Registry | None = None) -> dict:
    """MERGE bronze ``derivatives_quote`` series into ``silver.fact_derivatives_quote``."""
    reg = registry or load_registry()
    return _conform_b3_fact(
        spark, reg, fact="derivatives_quote", table="fact_derivatives_quote", cols=_DQ_COLS, ddl=_FACT_DERIV_QUOTE_DDL
    )


def build_series_metrics(spark: "SparkSession") -> dict:
    """Derive ``silver.series_metrics`` (pct change + rolling avg/vol) via windows."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    fact_uri = to_spark_path(silver_uri("fact_observation"))
    fact = spark.read.format("delta").load(fact_uri)

    w = Window.partitionBy("series_id").orderBy("date")
    w3 = w.rowsBetween(-2, 0)
    w12 = w.rowsBetween(-11, 0)

    metrics = (
        fact.select("series_id", "date", "value")
        .withColumn("pct_change", (F.col("value") / F.lag("value").over(w) - 1) * 100)
        .withColumn("rolling_3_avg", F.avg("value").over(w3))
        .withColumn("rolling_12_avg", F.avg("value").over(w12))
        .withColumn("rolling_12_vol", F.stddev("value").over(w12))
    )

    uri = to_spark_path(silver_uri("series_metrics"))
    metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(uri)
    log.info("series_metrics_built", uri=uri)
    return {"uri": uri}


def maintain(spark: "SparkSession", retain_hours: int = 168) -> None:
    """Compact + Z-ORDER the fact and reclaim old files (Delta housekeeping)."""
    fact_uri = to_spark_path(silver_uri("fact_observation"))
    spark.sql(f"OPTIMIZE delta.`{fact_uri}` ZORDER BY (series_id, date)")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM delta.`{fact_uri}` RETAIN {retain_hours} HOURS")
    log.info("silver_maintained", uri=fact_uri)


def run_silver(spark: "SparkSession", registry: Registry | None = None) -> dict:
    """Full silver build: dimensions + conformed fact + metrics."""
    from ofl.transform.spark.dimensions import build_dim_date, build_dim_instrument, build_dim_series

    reg = registry or load_registry()
    build_dim_series(spark, reg)
    build_dim_date(spark)
    build_dim_instrument(spark, reg)
    result = conform_observations(spark, reg)
    if result.get("merged", 0):
        build_series_metrics(spark)
    else:
        log.warning("silver_metrics_skipped", reason="no fact_observation")
    conform_treasury(spark, reg)
    conform_security_prices(spark, reg)
    conform_open_interest(spark, reg)
    conform_derivatives_quotes(spark, reg)
    return result
