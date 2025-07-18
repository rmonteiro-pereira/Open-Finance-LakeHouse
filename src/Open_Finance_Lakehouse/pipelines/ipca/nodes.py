"""
IPCA Pipeline - Brazilian Inflation Index
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
def aggregate_ipca_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create IPCA-specific gold layer with inflation KPIs
    """
    logger.info("[GOLD] Creating IPCA gold layer aggregations...")
    
    # Monthly aggregations for IPCA
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_ipca"),
        min("rate").alias("min_ipca"),
        max("rate").alias("max_ipca"),
        stddev("rate").alias("stddev_ipca")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_ipca") * 0 + "IPCA")
    
    logger.info(f"[GOLD] IPCA gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_ipca_raw(series_id: int, end_date: str = None) -> str:
    """IPCA raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_ipca_raw_to_bronze(raw_ipca: str) -> DataFrame:
    """Transform IPCA raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_ipca, 433)  # IPCA series ID


def transform_ipca_bronze_to_silver(bronze_ipca: DataFrame) -> DataFrame:
    """Transform IPCA bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_ipca, 433)


def validate_ipca_data(silver_ipca: DataFrame) -> dict:
    """Validate IPCA data with inflation-specific rules"""
    return validate_bacen_data_generic(silver_ipca, 433, max_rate=50.0)
