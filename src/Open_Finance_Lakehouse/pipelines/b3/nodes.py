"""
B3 Pipeline Nodes
Implements unified data extraction, processing and validation for B3 stock exchange data
Following the same pattern as Tesouro Direto and Yahoo Finance pipelines
"""

import json
import logging
from typing import Any
from datetime import datetime
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from ...utils.b3_api import fetch_b3_series

logger = logging.getLogger(__name__)


def extract_b3_all_series_raw(parameters: dict[str, Any]) -> dict[str, str]:
    """
    Extract raw data from all configured B3 series
    
    Args:
        parameters: B3 configuration parameters from parameters_b3.yml
        
    Returns:
        Dictionary with series names as keys and raw JSON data as values
    """
    logger.info("🔄 Starting extraction of all B3 series")
    
    # Parameters are already scoped to 'b3' section from pipeline
    series_config = parameters.get("series", {})
    
    raw_data_container = {}
    
    for series_name, series_info in series_config.items():
        logger.info(f"📊 Extracting B3 series: {series_name} ({series_info.get('symbol', 'N/A')})")
        
        try:
            # Fetch data using the utility function
            raw_data = fetch_b3_series(series_name)
            
            if raw_data and raw_data != "[]":
                # Validate JSON format
                parsed_data = json.loads(raw_data)
                logger.info(f"✅ Successfully extracted {len(parsed_data)} records for {series_name}")
                raw_data_container[series_name] = raw_data
            else:
                logger.warning(f"⚠️ No data retrieved for B3 series: {series_name}")
                raw_data_container[series_name] = "[]"
                
        except Exception as e:
            logger.error(f"❌ Error extracting B3 series {series_name}: {str(e)}")
            raw_data_container[series_name] = "[]"
    
    logger.info(f"✅ B3 extraction completed. {len(raw_data_container)} series processed")
    return raw_data_container


def process_b3_bronze_layer(raw_data_container: dict[str, str], parameters: dict[str, Any]) -> SparkDataFrame:
    """
    Process raw B3 data into bronze layer with unified schema
    
    Args:
        raw_data_container: Dictionary containing raw JSON data for all series
        parameters: B3 configuration parameters
        
    Returns:
        Spark DataFrame with bronze layer data
    """
    logger.info("🔄 Processing B3 data to bronze layer")
    
    spark = SparkSession.getActiveSession()
    if not spark:
        spark = SparkSession.builder.appName("B3Bronze").getOrCreate()
    
    # Define unified schema for bronze layer
    bronze_schema = StructType([
        StructField("series", StringType(), False),
        StructField("symbol", StringType(), True),
        StructField("date", StringType(), False),
        StructField("value", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("change_pct", DoubleType(), True),
        StructField("index_type", StringType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("extraction_timestamp", StringType(), False)
    ])
    
    # Parameters are already scoped to 'b3' section from pipeline
    series_config = parameters.get("series", {})
    
    all_bronze_data = []
    extraction_timestamp = datetime.now().isoformat()
    
    for series_name, raw_json in raw_data_container.items():
        if raw_json == "[]":
            logger.warning(f"⚠️ Skipping {series_name} - no data available")
            continue
            
        try:
            # Parse raw JSON data
            raw_records = json.loads(raw_json)
            series_info = series_config.get(series_name, {})
            
            logger.info(f"📊 Processing {len(raw_records)} records for {series_name}")
            
            for record in raw_records:
                # Map B3 data to bronze schema
                bronze_record = {
                    "series": series_name,
                    "symbol": series_info.get("symbol", ""),
                    "date": record.get("date", ""),
                    "value": float(record.get("value", 0)) if record.get("value") is not None else None,
                    "volume": float(record.get("volume", 0)) if record.get("volume") is not None else None,
                    "market_cap": float(record.get("market_cap", 0)) if record.get("market_cap") is not None else None,
                    "change_pct": float(record.get("change_pct", 0)) if record.get("change_pct") is not None else None,
                    "index_type": series_info.get("index_type", ""),
                    "category": series_info.get("category", ""),
                    "description": series_info.get("description", ""),
                    "extraction_timestamp": extraction_timestamp
                }
                all_bronze_data.append(bronze_record)
                
        except Exception as e:
            logger.error(f"❌ Error processing {series_name}: {str(e)}")
            continue
    
    if not all_bronze_data:
        logger.warning("⚠️ No bronze data to process, creating empty DataFrame")
        return spark.createDataFrame([], bronze_schema)
    
    # Create Spark DataFrame
    bronze_df = spark.createDataFrame(all_bronze_data, bronze_schema)
    
    logger.info(f"✅ Bronze layer processing completed. {bronze_df.count()} total records processed")
    
    # Show sample data for debugging
    logger.info("📊 Sample bronze data:")
    bronze_df.show(10, truncate=False)
    
    return bronze_df


def process_b3_silver_layer(bronze_df: SparkDataFrame, parameters: dict[str, Any]) -> SparkDataFrame:
    """
    Process bronze layer data to silver layer with data quality improvements
    
    Args:
        bronze_df: Bronze layer Spark DataFrame
        parameters: B3 configuration parameters
        
    Returns:
        Cleaned and enriched silver layer DataFrame
    """
    logger.info("🔄 Processing B3 data to silver layer")
    
    if bronze_df.count() == 0:
        logger.warning("⚠️ No bronze data to process for silver layer")
        return bronze_df
    
    # Convert date string to proper date type
    silver_df = bronze_df.withColumn(
        "date_parsed", 
        to_date(col("date"), "yyyy-MM-dd")
    )
    
    # Apply data quality filters
    quality_filters = (
        col("value").isNotNull() &
        (col("value") > 0) &
        col("date_parsed").isNotNull()
    )
    
    silver_df = silver_df.filter(quality_filters)
    
    # Add calculated fields for financial analysis
    silver_df = silver_df.withColumn(
        "value_normalized",
        when(col("value") > 0, col("value") / 1000).otherwise(None)
    )
    
    # Add volatility indicator based on change percentage
    silver_df = silver_df.withColumn(
        "volatility_category",
        when(col("change_pct").isNull(), "unknown")
        .when(col("change_pct").between(-1, 1), "low")
        .when(col("change_pct").between(-3, 3), "medium")
        .otherwise("high")
    )
    
    # Add processing timestamp
    silver_df = silver_df.withColumn(
        "processing_timestamp",
        lit(datetime.now().isoformat())
    )
    
    logger.info(f"✅ Silver layer processing completed. {silver_df.count()} records after quality filters")
    
    # Show sample data for debugging
    logger.info("📊 Sample silver data:")
    silver_df.select(
        "series", "symbol", "date_parsed", "value", "volume", 
        "change_pct", "volatility_category"
    ).show(10, truncate=False)
    
    return silver_df


def create_b3_catalog_entry(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Create catalog entry for B3 data
    
    Args:
        parameters: B3 configuration parameters
        
    Returns:
        Dictionary containing catalog metadata
    """
    # Parameters are already scoped to 'b3' section from pipeline
    series_config = parameters.get("series", {})
    
    catalog_entry = {
        "source": "b3",
        "description": "Brazilian stock exchange data from B3 including indices (IBOV, IFIX, IMOB, ICON, IEE), trading volume, and ETF bulletins",
        "data_format": "JSON/Delta",
        "update_frequency": "daily",
        "bronze_location": "s3a://lakehouse/bronze/b3/",
        "silver_location": "s3a://lakehouse/silver/b3/",
        "series_count": len(series_config),
        "series_list": list(series_config.keys()),
        "categories": {
            "index": [name for name, info in series_config.items() if info.get("category") == "index"],
            "volume": [name for name, info in series_config.items() if info.get("category") == "volume"],
            "etf": [name for name, info in series_config.items() if info.get("category") == "etf"]
        },
        "last_updated": datetime.now().isoformat(),
        "schema": {
            "bronze": [
                "series", "symbol", "date", "value", "volume", "market_cap",
                "change_pct", "index_type", "category", "description", 
                "extraction_timestamp"
            ],
            "silver": [
                "series", "symbol", "date_parsed", "value", "volume", "market_cap",
                "change_pct", "index_type", "category", "description",
                "value_normalized", "volatility_category",
                "extraction_timestamp", "processing_timestamp"
            ]
        }
    }
    
    logger.info(f"✅ Created B3 catalog entry with {len(series_config)} series")
    return catalog_entry
