"""
SELIC Meta Pipeline - Brazilian Financial Series
"""
from ..common.nodes import (
    ingest_bacen_raw,
    transform_raw_to_bronze_generic,
    transform_bronze_to_silver_generic,
    validate_bacen_data_generic,
    timing_decorator
)
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, min, max, stddev
import logging

logger = logging.getLogger(__name__)


@timing_decorator
def aggregate_selic_meta_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create SELIC Meta-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating SELIC Meta gold layer aggregations...")
    
    # Monthly aggregations for SELIC Meta
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_selic_meta"),
        min("rate").alias("min_selic_meta"),
        max("rate").alias("max_selic_meta"),
        stddev("rate").alias("stddev_selic_meta")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_selic_meta") * 0 + "SELIC Meta")
    
    logger.info(f"[GOLD] SELIC Meta gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_selic_meta_raw(series_id: int, end_date: str = None) -> str:
    """SELIC Meta raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_selic_meta_raw_to_bronze(raw_selic_meta: str) -> DataFrame:
    """Transform SELIC Meta raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_selic_meta, 432)


def transform_selic_meta_bronze_to_silver(bronze_selic_meta: DataFrame) -> DataFrame:
    """Transform SELIC Meta bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_selic_meta, 432)


def validate_selic_meta_data(silver_selic_meta: DataFrame) -> dict:
    """Validate SELIC Meta data with series-specific rules"""
    return validate_bacen_data_generic(silver_selic_meta, 432, max_rate=100.0)
