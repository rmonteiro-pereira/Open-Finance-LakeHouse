"""
INPC Pipeline - Brazilian Financial Series
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
def aggregate_inpc_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create INPC-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating INPC gold layer aggregations...")
    
    # Monthly aggregations for INPC
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_inpc"),
        min("rate").alias("min_inpc"),
        max("rate").alias("max_inpc"),
        stddev("rate").alias("stddev_inpc")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_inpc") * 0 + "INPC")
    
    logger.info(f"[GOLD] INPC gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_inpc_raw(series_id: int, end_date: str = None) -> str:
    """INPC raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_inpc_raw_to_bronze(raw_inpc: str) -> DataFrame:
    """Transform INPC raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_inpc, 188)


def transform_inpc_bronze_to_silver(bronze_inpc: DataFrame) -> DataFrame:
    """Transform INPC bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_inpc, 188)


def validate_inpc_data(silver_inpc: DataFrame) -> dict:
    """Validate INPC data with series-specific rules"""
    return validate_bacen_data_generic(silver_inpc, 188, max_rate=50.0)
