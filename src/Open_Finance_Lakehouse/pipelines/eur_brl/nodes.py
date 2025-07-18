"""
EUR/BRL Pipeline - Brazilian Financial Series
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
def aggregate_eur_brl_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create EUR/BRL-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating EUR/BRL gold layer aggregations...")
    
    # Monthly aggregations for EUR/BRL
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_eur_brl"),
        min("rate").alias("min_eur_brl"),
        max("rate").alias("max_eur_brl"),
        stddev("rate").alias("stddev_eur_brl")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_eur_brl") * 0 + "EUR/BRL")
    
    logger.info(f"[GOLD] EUR/BRL gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_eur_brl_raw(series_id: int, end_date: str = None) -> str:
    """EUR/BRL raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_eur_brl_raw_to_bronze(raw_eur_brl: str) -> DataFrame:
    """Transform EUR/BRL raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_eur_brl, 21619)


def transform_eur_brl_bronze_to_silver(bronze_eur_brl: DataFrame) -> DataFrame:
    """Transform EUR/BRL bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_eur_brl, 21619)


def validate_eur_brl_data(silver_eur_brl: DataFrame) -> dict:
    """Validate EUR/BRL data with series-specific rules"""
    return validate_bacen_data_generic(silver_eur_brl, 21619, max_rate=25.0)
