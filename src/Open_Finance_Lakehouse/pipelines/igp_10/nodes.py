"""
IGP-10 Pipeline - Brazilian Financial Series
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
def aggregate_igp_10_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create IGP-10-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating IGP-10 gold layer aggregations...")
    
    # Monthly aggregations for IGP-10
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_igp_10"),
        min("rate").alias("min_igp_10"),
        max("rate").alias("max_igp_10"),
        stddev("rate").alias("stddev_igp_10")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_igp_10") * 0 + "IGP-10")
    
    logger.info(f"[GOLD] IGP-10 gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_igp_10_raw(series_id: int, end_date: str = None) -> str:
    """IGP-10 raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_igp_10_raw_to_bronze(raw_igp_10: str) -> DataFrame:
    """Transform IGP-10 raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_igp_10, 7447)


def transform_igp_10_bronze_to_silver(bronze_igp_10: DataFrame) -> DataFrame:
    """Transform IGP-10 bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_igp_10, 7447)


def validate_igp_10_data(silver_igp_10: DataFrame) -> dict:
    """Validate IGP-10 data with series-specific rules"""
    return validate_bacen_data_generic(silver_igp_10, 7447, max_rate=50.0)
