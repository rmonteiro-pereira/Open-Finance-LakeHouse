"""
TLP Pipeline - Brazilian Financial Series
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
def aggregate_tlp_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create TLP-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating TLP gold layer aggregations...")
    
    # Monthly aggregations for TLP
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_tlp"),
        min("rate").alias("min_tlp"),
        max("rate").alias("max_tlp"),
        stddev("rate").alias("stddev_tlp")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_tlp") * 0 + "TLP")
    
    logger.info(f"[GOLD] TLP gold aggregation complete: {gold_df.count()} monthly aggregations")
    
    return gold_df


def ingest_tlp_raw(series_id: int, end_date: str = None) -> str:
    """TLP raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_tlp_raw_to_bronze(raw_tlp: str) -> DataFrame:
    """Transform TLP raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_tlp, 26192)


def transform_tlp_bronze_to_silver(bronze_tlp: DataFrame) -> DataFrame:
    """Transform TLP bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_tlp, 26192)


def validate_tlp_data(silver_tlp: DataFrame) -> dict:
    """Validate TLP data with series-specific rules"""
    return validate_bacen_data_generic(silver_tlp, 26192, max_rate=100.0)
