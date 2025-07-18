"""
CDI Pipeline - Brazilian Financial Series
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
def aggregate_cdi_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create CDI-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating CDI gold layer aggregations...")
    
    # Monthly aggregations for CDI
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_cdi"),
        min("rate").alias("min_cdi"),
        max("rate").alias("max_cdi"),
        stddev("rate").alias("stddev_cdi")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_cdi") * 0 + "CDI")
    
    logger.info(f"[GOLD] CDI gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_cdi_raw(series_id: int, end_date: str = None) -> str:
    """CDI raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_cdi_raw_to_bronze(raw_cdi: str) -> DataFrame:
    """Transform CDI raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_cdi, 12)


def transform_cdi_bronze_to_silver(bronze_cdi: DataFrame) -> DataFrame:
    """Transform CDI bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_cdi, 12)


def validate_cdi_data(silver_cdi: DataFrame) -> dict:
    """Validate CDI data with series-specific rules"""
    return validate_bacen_data_generic(silver_cdi, 12, max_rate=100.0)
