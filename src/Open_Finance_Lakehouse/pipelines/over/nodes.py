"""
OVER Pipeline - Brazilian Financial Series
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
def aggregate_over_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create OVER-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating OVER gold layer aggregations...")
    
    # Monthly aggregations for OVER
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_over"),
        min("rate").alias("min_over"),
        max("rate").alias("max_over"),
        stddev("rate").alias("stddev_over")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_over") * 0 + "OVER")
    
    logger.info(f"[GOLD] OVER gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_over_raw(series_id: int, end_date: str = None) -> str:
    """OVER raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_over_raw_to_bronze(raw_over: str) -> DataFrame:
    """Transform OVER raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_over, 1178)


def transform_over_bronze_to_silver(bronze_over: DataFrame) -> DataFrame:
    """Transform OVER bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_over, 1178)


def validate_over_data(silver_over: DataFrame) -> dict:
    """Validate OVER data with series-specific rules"""
    return validate_bacen_data_generic(silver_over, 1178, max_rate=100.0)
