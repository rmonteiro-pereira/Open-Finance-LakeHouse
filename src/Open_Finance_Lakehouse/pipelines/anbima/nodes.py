"""
ANBIMA Pipeline Nodes
Data processing nodes for ANBIMA financial market data
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, to_date, when
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from open_finance_lakehouse.utils.anbima_api import fetch_anbima_series

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
HIGH_RATE_THRESHOLD = 50


@dataclass
class ANBIMARawData:
    """Container for ANBIMA raw data from all series"""
    cdi: str
    ima_b: str
    ima_b_5: str
    curva_di: str
    curva_pre: str
    curva_ipca: str
    ntn_b: str
    ltn: str
    mercado_secundario: str


def get_spark_session() -> SparkSession:
    """Get or create Spark session for ANBIMA processing"""
    return SparkSession.builder.appName("ANBIMALakeHouse").getOrCreate()


# ANBIMA Data Extraction Functions
def extract_anbima_cdi_raw() -> str:
    """Extract CDI data from ANBIMA API"""
    logger.info("🏦 Extracting ANBIMA CDI data...")
    return fetch_anbima_series("cdi")


def extract_anbima_ima_b_raw() -> str:
    """Extract IMA-B data from ANBIMA API"""
    logger.info("📊 Extracting ANBIMA IMA-B data...")
    return fetch_anbima_series("ima_b")


def extract_anbima_ima_b_5_raw() -> str:
    """Extract IMA-B 5 years data from ANBIMA API"""
    logger.info("📈 Extracting ANBIMA IMA-B 5 years data...")
    return fetch_anbima_series("ima_b_5")


def extract_anbima_curva_di_raw() -> str:
    """Extract DI yield curve data from ANBIMA API"""
    logger.info("📉 Extracting ANBIMA DI yield curve data...")
    return fetch_anbima_series("curva_di")


def extract_anbima_curva_pre_raw() -> str:
    """Extract Pre-fixed yield curve data from ANBIMA API"""
    logger.info("📊 Extracting ANBIMA Pre-fixed yield curve data...")
    return fetch_anbima_series("curva_pre")


def extract_anbima_curva_ipca_raw() -> str:
    """Extract IPCA+ yield curve data from ANBIMA API"""
    logger.info("💹 Extracting ANBIMA IPCA+ yield curve data...")
    return fetch_anbima_series("curva_ipca")


def extract_anbima_ntn_b_raw() -> str:
    """Extract NTN-B data from ANBIMA API"""
    logger.info("🏛️ Extracting ANBIMA NTN-B data...")
    return fetch_anbima_series("ntn_b")


def extract_anbima_ltn_raw() -> str:
    """Extract LTN data from ANBIMA API"""
    logger.info("💰 Extracting ANBIMA LTN data...")
    return fetch_anbima_series("ltn")


def extract_anbima_mercado_secundario_raw() -> str:
    """Extract secondary market data from ANBIMA API"""
    logger.info("🏪 Extracting ANBIMA secondary market data...")
    return fetch_anbima_series("mercado_secundario")


# Bronze Layer Processing
def process_anbima_bronze(raw_data: ANBIMARawData) -> DataFrame:
    """
    Process ANBIMA raw data into bronze layer
    
    Args:
        raw_data: Container with all raw JSON strings from ANBIMA API
        
    Returns:
        Spark DataFrame with bronze ANBIMA data
    """
    logger.info("🔄 Processing ANBIMA data to bronze layer...")
    
    spark = get_spark_session()
    all_records = []
    
    # Map of raw data inputs
    raw_data_map = {
        "cdi": raw_data.cdi,
        "ima_b": raw_data.ima_b,
        "ima_b_5": raw_data.ima_b_5,
        "curva_di": raw_data.curva_di,
        "curva_pre": raw_data.curva_pre,
        "curva_ipca": raw_data.curva_ipca,
        "ntn_b": raw_data.ntn_b,
        "ltn": raw_data.ltn,
        "mercado_secundario": raw_data.mercado_secundario,
    }
    
    # Process each series
    for series_name, series_raw_data in raw_data_map.items():
        try:
            if series_raw_data and series_raw_data != "[]":
                records = json.loads(series_raw_data)
                for record in records:
                    # Standardize record format
                    standardized_record = {
                        "date": record.get("date"),
                        "rate": float(record.get("rate", 0)),
                        "series": record.get("series", series_name),
                        "source": "ANBIMA",
                        "raw_date": record.get("raw_date"),
                        "ingested_at": record.get("ingested_at"),
                        "index_value": record.get("index_value"),
                        "daily_return": record.get("daily_return"),
                        "vertex": record.get("vertex"),
                        "yield": record.get("yield"),
                        "maturity": record.get("maturity"),
                        "yield_to_maturity": record.get("yield_to_maturity"),
                    }
                    all_records.append(standardized_record)
                    
        except Exception as e:
            logger.error(f"Error processing {series_name}: {e}")
            continue
    
    if not all_records:
        logger.warning("No ANBIMA records to process")
        # Return empty DataFrame with schema
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("rate", DoubleType(), True),
            StructField("series", StringType(), True),
            StructField("source", StringType(), True),
            StructField("raw_date", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("index_value", DoubleType(), True),
            StructField("daily_return", DoubleType(), True),
            StructField("vertex", StringType(), True),
            StructField("yield", DoubleType(), True),
            StructField("maturity", StringType(), True),
            StructField("yield_to_maturity", DoubleType(), True),
        ])
        return spark.createDataFrame([], schema)
    
    # Define schema for mixed data    
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("series", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("reference_date", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("index_value", DoubleType(), True),
        StructField("daily_return", DoubleType(), True),
        StructField("vertex", StringType(), True),
        StructField("yield", DoubleType(), True),
        StructField("maturity", StringType(), True),
        StructField("yield_to_maturity", DoubleType(), True),
    ])
    
    # Create DataFrame with explicit schema
    df = spark.createDataFrame(all_records, schema)
    
    # Add processing metadata
    df = df.withColumn("processed_at", lit(datetime.now().isoformat()))
    df = df.withColumn("layer", lit("bronze"))
    
    logger.info(f"✅ Processed {df.count()} ANBIMA records to bronze layer")
    return df


# Silver Layer Processing
def process_anbima_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Process ANBIMA bronze data into silver layer
    
    Args:
        bronze_df: Bronze layer DataFrame
        
    Returns:
        Spark DataFrame with silver ANBIMA data
    """
    logger.info("✨ Processing ANBIMA data to silver layer...")
    
    if bronze_df.count() == 0:
        logger.warning("No bronze data to process to silver")
        return bronze_df.withColumn("layer", lit("silver"))
    
    # More relaxed data quality filters
    silver_df = (
        bronze_df
        .filter(col("date").isNotNull())
        .filter(col("value").isNotNull() | col("yield").isNotNull() | col("index_value").isNotNull())
        # Remove strict filter on value >= 0 for now
        .withColumn("date_parsed", to_date(col("date"), "yyyy-MM-dd"))
        .withColumn(
            "data_quality",
            when(col("value") > HIGH_RATE_THRESHOLD, "high_rate")
            .when(col("value") < 0, "negative_rate")
            .otherwise("normal")
        )
        .withColumn("layer", lit("silver"))
        .withColumn("processed_at", lit(datetime.now().isoformat()))
    )
    
    logger.info(f"✅ Processed {silver_df.count()} ANBIMA records to silver layer")
    return silver_df


# Catalog Functions
def create_anbima_catalog_entry(silver_df: DataFrame) -> dict[str, Any]:
    """
    Create catalog entry for ANBIMA data
    
    Args:
        silver_df: Silver layer DataFrame
        
    Returns:
        Dictionary with catalog metadata
    """
    logger.info("📋 Creating ANBIMA catalog entry...")
    
    if silver_df.count() == 0:
        logger.warning("No silver data for catalog")
        return {"dataset": "anbima_financial_data", "status": "empty"}
    
    # Get series statistics
    series_stats = silver_df.groupBy("series").count().collect()
    
    catalog_entry = {
        "dataset": "anbima_financial_data",
        "source": "ANBIMA",
        "description": "Brazilian financial market data from ANBIMA",
        "total_records": silver_df.count(),
        "series_count": len(series_stats),
        "series_breakdown": {row["series"]: row["count"] for row in series_stats},
        "date_range": {
            "min_date": silver_df.agg({"date": "min"}).collect()[0][0],
            "max_date": silver_df.agg({"date": "max"}).collect()[0][0],
        },
        "schema": silver_df.schema.json(),
        "created_at": datetime.now().isoformat(),
        "layer": "silver",
        "data_types": [
            "interest_rates",
            "bond_indices", 
            "yield_curves",
            "government_bonds",
            "secondary_market"
        ],
        "update_frequency": "daily",
        "business_days_only": True,
    }
    
    logger.info(f"✅ Created catalog for ANBIMA with {catalog_entry['total_records']} records")
    return catalog_entry
