"""
IGP-DI Pipeline - Brazilian Financial Series
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
def aggregate_igp_di_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create IGP-DI-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating IGP-DI gold layer aggregations...")
    
    # Monthly aggregations for IGP-DI
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_igp_di"),
        min("rate").alias("min_igp_di"),
        max("rate").alias("max_igp_di"),
        stddev("rate").alias("stddev_igp_di")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_igp_di") * 0 + "IGP-DI")
    
    logger.info(f"[GOLD] IGP-DI gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_igp_di_raw(series_id: int, end_date: str = None) -> str:
    """IGP-DI raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_igp_di_raw_to_bronze(raw_igp_di: str) -> DataFrame:
    """Transform IGP-DI raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_igp_di, 189)


def transform_igp_di_bronze_to_silver(bronze_igp_di: DataFrame) -> DataFrame:
    """Transform IGP-DI bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_igp_di, 189)


def validate_igp_di_data(silver_igp_di: DataFrame) -> dict:
    """Validate IGP-DI data with series-specific rules"""
    return validate_bacen_data_generic(silver_igp_di, 189, max_rate=50.0)
