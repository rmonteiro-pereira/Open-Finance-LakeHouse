"""
Tesouro Direto Pipeline Nodes
Implements unified data extraction, processing and validation for Tesouro Direto data
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

from ...utils.tesouro_direto_api import fetch_tesouro_series

logger = logging.getLogger(__name__)


def extract_tesouro_all_series_raw(parameters: dict[str, Any]) -> dict[str, str]:
    """
    Extract raw data from all configured Tesouro Direto series
    
    Args:
        parameters: Tesouro Direto configuration parameters from parameters_tesouro_direto.yml
        
    Returns:
        Dictionary with series names as keys and raw JSON data as values
    """
    logger.info("🔄 Starting extraction of all Tesouro Direto series")
    
    # Get series configuration from parameters
    series_config = parameters.get("series", {})
    
    raw_data_container = {}
    
    for series_name, series_info in series_config.items():
        logger.info(f"💰 Extracting Tesouro Direto series: {series_name} ({series_info.get('title_code', 'N/A')})")
        
        try:
            # Fetch data using the utility function
            raw_data = fetch_tesouro_series(series_name)
            
            if raw_data and raw_data != "[]":
                # Validate JSON format
                parsed_data = json.loads(raw_data)
                logger.info(f"✅ Successfully extracted {len(parsed_data)} records for {series_name}")
                raw_data_container[series_name] = raw_data
            else:
                logger.warning(f"⚠️ No data retrieved for Tesouro Direto series: {series_name}")
                raw_data_container[series_name] = "[]"
                
        except Exception as e:
            logger.error(f"❌ Error extracting Tesouro Direto series {series_name}: {str(e)}")
            raw_data_container[series_name] = "[]"
    
    logger.info(f"✅ Tesouro Direto extraction completed. {len(raw_data_container)} series processed")
    return raw_data_container


def process_tesouro_bronze_layer(raw_data_container: dict[str, str], parameters: dict[str, Any]) -> SparkDataFrame:
    """
    Process raw Tesouro Direto data into bronze layer with unified schema
    
    Args:
        raw_data_container: Dictionary containing raw JSON data for all series
        parameters: Tesouro Direto configuration parameters
        
    Returns:
        Spark DataFrame with bronze layer data
    """
    logger.info("🔄 Processing Tesouro Direto data to bronze layer")
    
    spark = SparkSession.getActiveSession()
    if not spark:
        spark = SparkSession.builder.appName("TesouroDirectoBronze").getOrCreate()
    
    # Define unified schema for bronze layer
    bronze_schema = StructType([
        StructField("series", StringType(), False),
        StructField("title_code", StringType(), True),
        StructField("date", StringType(), False),
        StructField("title_type", StringType(), True),
        StructField("maturity_date", StringType(), True),
        StructField("buy_rate", DoubleType(), True),
        StructField("sell_rate", DoubleType(), True),
        StructField("buy_price", DoubleType(), True),
        StructField("sell_price", DoubleType(), True),
        StructField("min_investment", DoubleType(), True),
        StructField("type", StringType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("extraction_timestamp", StringType(), False)
    ])
    
    # Get series configuration
    tesouro_config = parameters.get("tesouro_direto", {})
    series_config = tesouro_config.get("series", {})
    
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
            
            logger.info(f"💰 Processing {len(raw_records)} records for {series_name}")
            
            for record in raw_records:
                # Map Tesouro Direto data to bronze schema
                bronze_record = {
                    "series": series_name,
                    "title_code": series_info.get("title_code", ""),
                    "date": record.get("date", ""),
                    "title_type": record.get("title_type", ""),
                    "maturity_date": record.get("maturity_date", ""),
                    "buy_rate": float(record.get("buy_rate", 0)) if record.get("buy_rate") is not None else None,
                    "sell_rate": float(record.get("sell_rate", 0)) if record.get("sell_rate") is not None else None,
                    "buy_price": float(record.get("buy_price", 0)) if record.get("buy_price") is not None else None,
                    "sell_price": float(record.get("sell_price", 0)) if record.get("sell_price") is not None else None,
                    "min_investment": float(record.get("min_investment", 0)) if record.get("min_investment") is not None else None,
                    "type": series_info.get("type", ""),
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
    logger.info("💰 Sample bronze data:")
    bronze_df.show(10, truncate=False)
    
    return bronze_df


def process_tesouro_silver_layer(bronze_df: SparkDataFrame, parameters: dict[str, Any]) -> SparkDataFrame:
    """
    Process bronze layer data to silver layer with data quality improvements
    
    Args:
        bronze_df: Bronze layer Spark DataFrame
        parameters: Tesouro Direto configuration parameters
        
    Returns:
        Cleaned and enriched silver layer DataFrame
    """
    logger.info("🔄 Processing Tesouro Direto data to silver layer")
    
    if bronze_df.count() == 0:
        logger.warning("⚠️ No bronze data to process for silver layer")
        return bronze_df
    
    # Convert date strings to proper date types
    silver_df = bronze_df.withColumn(
        "date_parsed", 
        to_date(col("date"), "dd/MM/yyyy")
    ).withColumn(
        "maturity_date_parsed",
        to_date(col("maturity_date"), "dd/MM/yyyy")  
    )
    
    # Apply data quality filters
    quality_filters = (
        col("date_parsed").isNotNull() &
        (
            col("buy_rate").isNotNull() | 
            col("sell_rate").isNotNull() |
            col("buy_price").isNotNull() |
            col("sell_price").isNotNull()
        )
    )
    
    silver_df = silver_df.filter(quality_filters)
    
    # Add calculated fields
    silver_df = silver_df.withColumn(
        "rate_spread", 
        when(
            col("buy_rate").isNotNull() & col("sell_rate").isNotNull(),
            col("sell_rate") - col("buy_rate")
        ).otherwise(None)
    )
    
    silver_df = silver_df.withColumn(
        "price_spread",
        when(
            col("buy_price").isNotNull() & col("sell_price").isNotNull(),
            col("sell_price") - col("buy_price")
        ).otherwise(None)
    )
    
    # Add days to maturity
    silver_df = silver_df.withColumn(
        "days_to_maturity",
        when(
            col("date_parsed").isNotNull() & col("maturity_date_parsed").isNotNull(),
            col("maturity_date_parsed").cast("long") - col("date_parsed").cast("long")
        ).otherwise(None)
    )
    
    # Add processing timestamp
    silver_df = silver_df.withColumn(
        "processing_timestamp",
        lit(datetime.now().isoformat())
    )
    
    logger.info(f"✅ Silver layer processing completed. {silver_df.count()} records after quality filters")
    
    # Show sample data for debugging
    logger.info("💰 Sample silver data:")
    silver_df.select(
        "series", "title_code", "date_parsed", "buy_rate", "sell_rate",
        "rate_spread", "days_to_maturity"
    ).show(10, truncate=False)
    
    return silver_df


def create_tesouro_catalog_entry(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Create catalog entry for Tesouro Direto data
    
    Args:
        parameters: Tesouro Direto configuration parameters
        
    Returns:
        Dictionary containing catalog metadata
    """
    tesouro_config = parameters.get("tesouro_direto", {})
    series_config = tesouro_config.get("series", {})
    
    catalog_entry = {
        "source": "tesouro_direto",
        "description": "Brazilian Treasury bonds data including rates, prices and maturities for Tesouro Prefixado, IPCA+ and Selic",
        "data_format": "JSON/Delta",
        "update_frequency": "daily",
        "bronze_location": "s3a://lakehouse/bronze/tesouro_direto/",
        "silver_location": "s3a://lakehouse/silver/tesouro_direto/",
        "series_count": len(series_config),
        "series_list": list(series_config.keys()),
        "bond_types": {
            "prefixado": [name for name, info in series_config.items() if info.get("type") == "prefixado"],
            "ipca_plus": [name for name, info in series_config.items() if info.get("type") == "ipca_plus"],
            "selic": [name for name, info in series_config.items() if info.get("type") == "selic"]
        },
        "last_updated": datetime.now().isoformat(),
        "schema": {
            "bronze": [
                "series", "title_code", "date", "title_type", "maturity_date",
                "buy_rate", "sell_rate", "buy_price", "sell_price", "min_investment",
                "type", "category", "description", "extraction_timestamp"
            ],
            "silver": [
                "series", "title_code", "date_parsed", "title_type", "maturity_date_parsed",
                "buy_rate", "sell_rate", "buy_price", "sell_price", "min_investment",
                "type", "category", "description", "rate_spread", "price_spread",
                "days_to_maturity", "extraction_timestamp", "processing_timestamp"
            ]
        }
    }
    
    logger.info(f"✅ Created Tesouro Direto catalog entry with {len(series_config)} series")
    return catalog_entry
