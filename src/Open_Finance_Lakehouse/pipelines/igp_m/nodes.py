"""
IGP-M Pipeline - Brazilian Financial Series
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
def aggregate_igp_m_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create IGP-M-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating IGP-M gold layer aggregations...")
    
    # Monthly aggregations for IGP-M
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_igp_m"),
        min("rate").alias("min_igp_m"),
        max("rate").alias("max_igp_m"),
        stddev("rate").alias("stddev_igp_m")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_igp_m") * 0 + "IGP-M")
    
    logger.info(f"[GOLD] IGP-M gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_igp_m_raw(series_id: int, end_date: str = None) -> str:
    """IGP-M raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_igp_m_raw_to_bronze(raw_igp_m: str) -> DataFrame:
    """Transform IGP-M raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_igp_m, 190)


def transform_igp_m_bronze_to_silver(bronze_igp_m: DataFrame) -> DataFrame:
    """Transform IGP-M bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_igp_m, 190)


def validate_igp_m_data(silver_igp_m: DataFrame) -> dict:
    """Validate IGP-M data with series-specific rules"""
    return validate_bacen_data_generic(silver_igp_m, 190, max_rate=50.0)
