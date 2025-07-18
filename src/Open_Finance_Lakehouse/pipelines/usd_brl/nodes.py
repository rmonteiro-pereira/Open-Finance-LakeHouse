"""
USD/BRL Pipeline - Brazilian Financial Series
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
def aggregate_usd_brl_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create USD/BRL-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating USD/BRL gold layer aggregations...")
    
    # Monthly aggregations for USD/BRL
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_usd_brl"),
        min("rate").alias("min_usd_brl"),
        max("rate").alias("max_usd_brl"),
        stddev("rate").alias("stddev_usd_brl")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_usd_brl") * 0 + "USD/BRL")
    
    logger.info(f"[GOLD] USD/BRL gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_usd_brl_raw(series_id: int, end_date: str = None) -> str:
    """USD/BRL raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_usd_brl_raw_to_bronze(raw_usd_brl: str) -> DataFrame:
    """Transform USD/BRL raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_usd_brl, 1)


def transform_usd_brl_bronze_to_silver(bronze_usd_brl: DataFrame) -> DataFrame:
    """Transform USD/BRL bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_usd_brl, 1)


def validate_usd_brl_data(silver_usd_brl: DataFrame) -> dict:
    """Validate USD/BRL data with series-specific rules"""
    return validate_bacen_data_generic(silver_usd_brl, 1, max_rate=20.0)
