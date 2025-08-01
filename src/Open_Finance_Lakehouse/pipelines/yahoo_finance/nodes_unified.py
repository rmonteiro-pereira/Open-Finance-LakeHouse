"""
Yahoo Finance Pipeline Nodes
Implements unified data extraction, processing and validation for Yahoo Finance data
Following the same pattern as ANBIMA pipeline
"""

import json
import logging
from typing import Any
from datetime import datetime
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from ...utils.yahoo_api import fetch_yahoo_series

logger = logging.getLogger(__name__)


def extract_yahoo_all_series_raw(parameters: dict[str, Any]) -> dict[str, str]:
    """
    Extract raw data from all configured Yahoo Finance series
    
    Args:
        parameters: Yahoo Finance configuration parameters from parameters_yahoo_finance.yml
        
    Returns:
        Dictionary with series names as keys and raw JSON data as values
    """
    logger.info("🔄 Starting extraction of all Yahoo Finance series")
    
    # Get series configuration from parameters
    yahoo_config = parameters.get("yahoo_finance", {})
    series_config = yahoo_config.get("series", {})
    
    raw_data_container = {}
    
    for series_name, series_info in series_config.items():
        logger.info(f"📊 Extracting Yahoo Finance series: {series_name} ({series_info.get('symbol', 'N/A')})")
        
        try:
            # Fetch data using the utility function
            raw_data = fetch_yahoo_series(series_name)
            
            if raw_data and raw_data != "[]":
                # Validate JSON format
                parsed_data = json.loads(raw_data)
                logger.info(f"✅ Successfully extracted {len(parsed_data)} records for {series_name}")
                raw_data_container[series_name] = raw_data
            else:
                logger.warning(f"⚠️ No data retrieved for Yahoo Finance series: {series_name}")
                raw_data_container[series_name] = "[]"
                
        except Exception as e:
            logger.error(f"❌ Error extracting Yahoo Finance series {series_name}: {str(e)}")
            raw_data_container[series_name] = "[]"
    
    logger.info(f"✅ Yahoo Finance extraction completed. {len(raw_data_container)} series processed")
    return raw_data_container


def process_yahoo_bronze_layer(raw_data_container: dict[str, str], parameters: dict[str, Any]) -> SparkDataFrame:
    """
    Process raw Yahoo Finance data into bronze layer with unified schema
    
    Args:
        raw_data_container: Dictionary containing raw JSON data for all series
        parameters: Yahoo Finance configuration parameters
        
    Returns:
        Spark DataFrame with bronze layer data
    """
    logger.info("🔄 Processing Yahoo Finance data to bronze layer")
    
    spark = SparkSession.getActiveSession()
    if not spark:
        spark = SparkSession.builder.appName("YahooFinanceBronze").getOrCreate()
    
    # Define unified schema for bronze layer
    bronze_schema = StructType([
        StructField("series", StringType(), False),
        StructField("symbol", StringType(), True),
        StructField("date", StringType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True), 
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("adj_close", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("extraction_timestamp", StringType(), False)
    ])
    
    # Get series configuration
    yahoo_config = parameters.get("yahoo_finance", {})
    series_config = yahoo_config.get("series", {})
    
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
                # Map Yahoo Finance data to bronze schema
                bronze_record = {
                    "series": series_name,
                    "symbol": series_info.get("symbol", ""),
                    "date": record.get("date", ""),
                    "open": float(record.get("open", 0)) if record.get("open") is not None else None,
                    "high": float(record.get("high", 0)) if record.get("high") is not None else None,
                    "low": float(record.get("low", 0)) if record.get("low") is not None else None,
                    "close": float(record.get("close", 0)) if record.get("close") is not None else None,
                    "volume": float(record.get("volume", 0)) if record.get("volume") is not None else None,
                    "adj_close": float(record.get("adjclose", 0)) if record.get("adjclose") is not None else None,
                    "currency": series_info.get("currency", ""),
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
    
    return bronze_df


def process_yahoo_silver_layer(bronze_df: SparkDataFrame, parameters: dict[str, Any]) -> SparkDataFrame:
    """
    Process bronze layer data to silver layer with data quality improvements
    
    Args:
        bronze_df: Bronze layer Spark DataFrame
        parameters: Yahoo Finance configuration parameters
        
    Returns:
        Cleaned and enriched silver layer DataFrame
    """
    logger.info("🔄 Processing Yahoo Finance data to silver layer")
    
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
        col("close").isNotNull() &
        (col("close") > 0) &
        col("date_parsed").isNotNull()
    )
    
    silver_df = silver_df.filter(quality_filters)
    
    # Add calculated fields
    silver_df = silver_df.withColumn(
        "daily_return_pct",
        when(col("open") > 0, ((col("close") - col("open")) / col("open") * 100)).otherwise(None)
    )
    
    # Add volatility indicator
    silver_df = silver_df.withColumn(
        "daily_volatility_pct",
        when(col("low") > 0, ((col("high") - col("low")) / col("low") * 100)).otherwise(None)
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
        "series", "symbol", "date_parsed", "close", "volume", 
        "daily_return_pct", "daily_volatility_pct"
    ).show(10, truncate=False)
    
    return silver_df


def create_yahoo_catalog_entry(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Create catalog entry for Yahoo Finance data
    
    Args:
        parameters: Yahoo Finance configuration parameters
        
    Returns:
        Dictionary containing catalog metadata
    """
    yahoo_config = parameters.get("yahoo_finance", {})
    series_config = yahoo_config.get("series", {})
    
    catalog_entry = {
        "source": "yahoo_finance",
        "description": "Financial market data from Yahoo Finance including Brazilian ETFs, commodities, and currency pairs",
        "data_format": "JSON/Delta",
        "update_frequency": "daily",
        "bronze_location": "s3a://lakehouse/bronze/yahoo_finance/",
        "silver_location": "s3a://lakehouse/silver/yahoo_finance/",
        "series_count": len(series_config),
        "series_list": list(series_config.keys()),
        "categories": {
            "etf": [name for name, info in series_config.items() if info.get("category") == "etf"],
            "commodity": [name for name, info in series_config.items() if info.get("category") == "commodity"],
            "currency": [name for name, info in series_config.items() if info.get("category") == "currency"]
        },
        "last_updated": datetime.now().isoformat(),
        "schema": {
            "bronze": [
                "series", "symbol", "date", "open", "high", "low", 
                "close", "volume", "adj_close", "currency", "category", 
                "description", "extraction_timestamp"
            ],
            "silver": [
                "series", "symbol", "date_parsed", "open", "high", "low",
                "close", "volume", "adj_close", "currency", "category",
                "description", "daily_return_pct", "daily_volatility_pct",
                "extraction_timestamp", "processing_timestamp"
            ]
        }
    }
    
    logger.info(f"✅ Created Yahoo Finance catalog entry with {len(series_config)} series")
    return catalog_entry
