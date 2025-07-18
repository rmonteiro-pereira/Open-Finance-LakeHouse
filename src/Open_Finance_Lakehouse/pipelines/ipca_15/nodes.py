"""
IPCA-15 Pipeline - Brazilian Financial Series
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
def aggregate_ipca_15_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create IPCA-15-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating IPCA-15 gold layer aggregations...")
    
    # Monthly aggregations for IPCA-15
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_ipca_15"),
        min("rate").alias("min_ipca_15"),
        max("rate").alias("max_ipca_15"),
        stddev("rate").alias("stddev_ipca_15")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_ipca_15") * 0 + "IPCA-15")
    
    logger.info(f"[GOLD] IPCA-15 gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_ipca_15_raw(series_id: int, end_date: str = None) -> str:
    """IPCA-15 raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_ipca_15_raw_to_bronze(raw_ipca_15: str) -> DataFrame:
    """Transform IPCA-15 raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_ipca_15, 1705)


def transform_ipca_15_bronze_to_silver(bronze_ipca_15: DataFrame) -> DataFrame:
    """Transform IPCA-15 bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_ipca_15, 1705)


def validate_ipca_15_data(silver_ipca_15: DataFrame) -> dict:
    """Validate IPCA-15 data with series-specific rules"""
    return validate_bacen_data_generic(silver_ipca_15, 1705, max_rate=50.0)
