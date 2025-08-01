"""
IPEA/Receita Federal Pipeline Nodes
Implements unified data extraction, processing and validation for Brazilian economic and fiscal data
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

from ...utils.ipea_receita_api import fetch_ipea_receita_series

logger = logging.getLogger(__name__)


def extract_ipea_receita_all_series_raw(parameters: dict[str, Any]) -> dict[str, str]:
    """
    Extract raw data from all configured IPEA/Receita Federal series
    
    Args:
        parameters: IPEA/Receita configuration parameters from parameters_ipea_receita.yml
        
    Returns:
        Dictionary with series names as keys and raw JSON data as values
    """
    logger.info("🔄 Starting extraction of all IPEA/Receita Federal series")
    
    # Parameters are already scoped to 'ipea_receita' section from pipeline
    series_config = parameters.get("series", {})
    
    raw_data_container = {}
    
    for series_name, series_info in series_config.items():
        logger.info(f"📊 Extracting IPEA/Receita series: {series_name} ({series_info.get('description', 'N/A')})")
        
        try:
            # Fetch data using the utility function
            raw_data = fetch_ipea_receita_series(series_name)
            
            if raw_data and raw_data != "[]":
                # Validate JSON format
                parsed_data = json.loads(raw_data)
                logger.info(f"✅ Successfully extracted {len(parsed_data)} records for {series_name}")
                raw_data_container[series_name] = raw_data
            else:
                logger.warning(f"⚠️ No data retrieved for IPEA/Receita series: {series_name}")
                raw_data_container[series_name] = "[]"
                
        except Exception as e:
            logger.error(f"❌ Error extracting IPEA/Receita series {series_name}: {str(e)}")
            raw_data_container[series_name] = "[]"
    
    logger.info(f"✅ IPEA/Receita extraction completed. {len(raw_data_container)} series processed")
    return raw_data_container


def process_ipea_receita_bronze_layer(raw_data_container: dict[str, str], parameters: dict[str, Any]) -> SparkDataFrame:
    """
    Process raw IPEA/Receita data into bronze layer with unified schema
    
    Args:
        raw_data_container: Dictionary containing raw JSON data for all series
        parameters: IPEA/Receita configuration parameters
        
    Returns:
        Spark DataFrame with bronze layer data
    """
    logger.info("🔄 Processing IPEA/Receita data to bronze layer")
    
    spark = SparkSession.getActiveSession()
    if not spark:
        spark = SparkSession.builder.appName("IPEAReceitaBronze").getOrCreate()
    
    # Define unified schema for bronze layer
    bronze_schema = StructType([
        StructField("series", StringType(), False),
        StructField("indicator_code", StringType(), True),
        StructField("date", StringType(), False),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("frequency", StringType(), True),
        StructField("source_agency", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("description", StringType(), True),
        StructField("extraction_timestamp", StringType(), False)
    ])
    
    # Parameters are already scoped to 'ipea_receita' section from pipeline
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
                # Map IPEA/Receita data to bronze schema
                bronze_record = {
                    "series": series_name,
                    "indicator_code": series_info.get("code", ""),
                    "date": record.get("date", ""),
                    "value": float(record.get("value", 0)) if record.get("value") is not None else None,
                    "unit": series_info.get("unit", ""),
                    "frequency": series_info.get("frequency", ""),
                    "source_agency": series_info.get("source", ""),
                    "category": series_info.get("category", ""),
                    "subcategory": series_info.get("subcategory", ""),
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


def process_ipea_receita_silver_layer(bronze_df: SparkDataFrame, parameters: dict[str, Any]) -> SparkDataFrame:
    """
    Process bronze layer data to silver layer with data quality improvements
    
    Args:
        bronze_df: Bronze layer Spark DataFrame
        parameters: IPEA/Receita configuration parameters
        
    Returns:
        Cleaned and enriched silver layer DataFrame
    """
    logger.info("🔄 Processing IPEA/Receita data to silver layer")
    
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
        col("date_parsed").isNotNull()
    )
    
    silver_df = silver_df.filter(quality_filters)
    
    # Add economic indicators classification
    silver_df = silver_df.withColumn(
        "indicator_type",
        when(col("category") == "fiscal", "fiscal")
        .when(col("category") == "tax", "taxation")
        .when(col("category") == "economic", "economic")
        .otherwise("other")
    )
    
    # Add value classification for fiscal indicators
    silver_df = silver_df.withColumn(
        "fiscal_impact",
        when(col("indicator_type") == "fiscal", 
             when(col("value") > 0, "positive")
             .when(col("value") < 0, "negative")
             .otherwise("neutral"))
        .otherwise("not_applicable")
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
        "series", "indicator_code", "date_parsed", "value", "unit", 
        "indicator_type", "fiscal_impact"
    ).show(10, truncate=False)
    
    return silver_df


def create_ipea_receita_catalog_entry(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Create catalog entry for IPEA/Receita Federal data
    
    Args:
        parameters: IPEA/Receita configuration parameters
        
    Returns:
        Dictionary containing catalog metadata
    """
    # Parameters are already scoped to 'ipea_receita' section from pipeline
    series_config = parameters.get("series", {})
    
    catalog_entry = {
        "source": "ipea_receita_federal",
        "description": "Brazilian economic and fiscal data from IPEA and Receita Federal including primary deficit, tax collection, public debt, productivity, public investment, tax burden, IRPJ, and CSLL",
        "data_format": "JSON/Delta",
        "update_frequency": "monthly",
        "bronze_location": "s3a://lakehouse/bronze/ipea_receita/",
        "silver_location": "s3a://lakehouse/silver/ipea_receita/",
        "series_count": len(series_config),
        "series_list": list(series_config.keys()),
        "categories": {
            "fiscal": [name for name, info in series_config.items() if info.get("category") == "fiscal"],
            "taxation": [name for name, info in series_config.items() if info.get("category") == "tax"],
            "economic": [name for name, info in series_config.items() if info.get("category") == "economic"]
        },
        "last_updated": datetime.now().isoformat(),
        "schema": {
            "bronze": [
                "series", "indicator_code", "date", "value", "unit", "frequency",
                "source_agency", "category", "subcategory", "description", 
                "extraction_timestamp"
            ],
            "silver": [
                "series", "indicator_code", "date_parsed", "value", "unit", "frequency",
                "source_agency", "category", "subcategory", "description",
                "indicator_type", "fiscal_impact",
                "extraction_timestamp", "processing_timestamp"
            ]
        }
    }
    
    logger.info(f"✅ Created IPEA/Receita catalog entry with {len(series_config)} series")
    return catalog_entry
