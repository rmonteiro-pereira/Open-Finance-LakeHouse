"""
Yahoo Finance Pipeline Nodes
Implements data extraction, processing and validation for Yahoo Finance data
"""

import json
import logging
import pandas as pd
from typing import Any
from datetime import datetime
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, lag, when, lit
from pyspark.sql.window import Window

from ...utils.yahoo_api import fetch_yahoo_series
from ...utils.data_quality import (
    ValidationResult,
    generate_quality_report
)

logger = logging.getLogger(__name__)


def extract_yahoo_series_raw(series_name: str, parameters: dict[str, Any]) -> str:
    """
    Extract raw data from Yahoo Finance API
    
    Args:
        series_name: Name of the Yahoo Finance series to extract
        parameters: Yahoo Finance configuration parameters
        
    Returns:
        JSON string containing raw data
    """
    logger.info(f"🔄 Starting raw data extraction for Yahoo Finance series: {series_name}")
    
    try:
        # Extract raw data using Yahoo Finance API
        raw_data = fetch_yahoo_series(series_name)
        
        if not raw_data or raw_data == "[]":
            logger.warning(f"⚠️ No data retrieved for Yahoo Finance series: {series_name}")
            return "[]"
        
        # Parse to validate JSON format
        parsed_data = json.loads(raw_data)
        logger.info(f"✅ Successfully extracted {len(parsed_data)} records for {series_name}")
        
        return raw_data
        
    except Exception as e:
        logger.error(f"❌ Error extracting Yahoo Finance series {series_name}: {e}")
        return "[]"


def process_yahoo_series_bronze(raw_data: str, series_name: str, parameters: dict[str, Any]):
    """
    Process raw Yahoo Finance data into Bronze layer using Spark
    
    Args:
        raw_data: Raw JSON data from Yahoo Finance API
        series_name: Name of the series being processed
        parameters: Yahoo Finance configuration parameters
        
    Returns:
        Spark DataFrame for Bronze layer
    """
    logger.info(f"🔄 Processing {series_name} data to Bronze layer")
    
    try:
        # Parse raw JSON data
        if not raw_data or raw_data == "[]":
            logger.warning(f"⚠️ No raw data to process for {series_name}")
            # Return empty Spark DataFrame
            spark = SparkSession.getActiveSession() or SparkSession.builder.appName("yahoo_bronze").getOrCreate()
            return spark.createDataFrame([], schema="")
            
        records = json.loads(raw_data)
        
        if not records:
            logger.warning(f"⚠️ Empty records list for {series_name}")
            spark = SparkSession.getActiveSession() or SparkSession.builder.appName("yahoo_bronze").getOrCreate()
            return spark.createDataFrame([], schema="")
        
        # Convert to Pandas DataFrame first for processing
        df = pd.DataFrame(records)
        
        # Add bronze layer metadata
        df["bronze_ingested_at"] = datetime.now().isoformat()
        df["bronze_source"] = "yahoo_finance"
        df["bronze_series_name"] = series_name
        
        # Ensure consistent column types
        df["date"] = pd.to_datetime(df["date"])
        df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
        
        # Sort by date
        df = df.sort_values("date").reset_index(drop=True)
        
        logger.info(f"✅ Processed {len(df)} records to Bronze layer for {series_name}")
        
        # Convert to Spark DataFrame
        spark = SparkSession.getActiveSession() or SparkSession.builder.appName("yahoo_bronze").getOrCreate()
        spark_df = spark.createDataFrame(df)
        
        return spark_df
        
    except Exception as e:
        logger.error(f"❌ Error processing {series_name} to Bronze layer: {e}")
        spark = SparkSession.getActiveSession() or SparkSession.builder.appName("yahoo_bronze").getOrCreate()
        return spark.createDataFrame([], schema="")


def validate_yahoo_series_bronze(df, series_name: str, parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Validate Yahoo Finance Bronze layer data
    
    Args:
        df: Bronze layer DataFrame (Spark DataFrame)
        series_name: Name of the series being validated
        parameters: Yahoo Finance configuration parameters
        
    Returns:
        Validation results dictionary
    """
    logger.info(f"🔍 Validating Bronze layer data for {series_name}")
    
    # Convert Spark DataFrame to pandas for validation
    if hasattr(df, 'toPandas'):
        df_pandas = df.toPandas()
    else:
        df_pandas = df
    
    # Get validation parameters
    validation_config = parameters.get("yahoo_finance", {}).get("processing", {}).get("validation_rules", {})
    min_records = validation_config.get("min_records", 100)
    max_null_percentage = validation_config.get("max_null_percentage", 5)
    date_range_days = validation_config.get("date_range_days", 30)

    # Define validation rules
    validation_rules = [
        {
            "name": "minimum_records",
            "description": f"Should have at least {min_records} records",
            "check": len(df_pandas) >= min_records,
            "severity": "error" if len(df_pandas) < min_records else "info",
            "details": f"Found {len(df_pandas)} records, minimum required: {min_records}"
        },
        {
            "name": "no_duplicate_dates",
            "description": "Should not have duplicate dates",
            "check": df_pandas["date"].nunique() == len(df_pandas) if len(df_pandas) > 0 and "date" in df_pandas.columns else True,
            "severity": "warning" if len(df_pandas) > 0 and "date" in df_pandas.columns and df_pandas["date"].nunique() != len(df_pandas) else "info",
            "details": f"Unique dates: {df_pandas['date'].nunique()}, Total records: {len(df_pandas)}" if len(df_pandas) > 0 and "date" in df_pandas.columns else "No data to check"
        },
        {
            "name": "rate_not_null",
            "description": f"Rate values should be less than {max_null_percentage}% null",
            "check": (df_pandas["rate"].isnull().sum() / len(df_pandas) * 100) <= max_null_percentage if len(df_pandas) > 0 and "rate" in df_pandas.columns else True,
            "severity": "error" if len(df_pandas) > 0 and "rate" in df_pandas.columns and (df_pandas["rate"].isnull().sum() / len(df_pandas) * 100) > max_null_percentage else "info",
            "details": f"Null rate percentage: {df_pandas['rate'].isnull().sum() / len(df_pandas) * 100:.2f}%" if len(df_pandas) > 0 and "rate" in df_pandas.columns else "No data to validate"
        },
        {
            "name": "date_range_coverage",
            "description": f"Should cover at least {date_range_days} days",
            "check": (df_pandas["date"].max() - df_pandas["date"].min()).days >= date_range_days if len(df_pandas) > 1 and "date" in df_pandas.columns else True,
            "severity": "warning" if len(df_pandas) > 1 and "date" in df_pandas.columns and (df_pandas["date"].max() - df_pandas["date"].min()).days < date_range_days else "info",
            "details": f"Date range: {(df_pandas['date'].max() - df_pandas['date'].min()).days} days" if len(df_pandas) > 1 and "date" in df_pandas.columns else "Insufficient data for range check"
        }
    ]
    
    # Execute validation
    validation_results = []
    for rule in validation_rules:
        result = ValidationResult(
            rule_name=rule["name"],
            passed=rule["check"],
            message=rule["details"],
            severity=rule["severity"]
        )
        validation_results.append(result)
    
    # Generate validation report
    report = generate_quality_report(validation_results, series_name)
    
    logger.info(f"✅ Validation completed for {series_name}: {report['summary']['passed']}/{report['summary']['total']} checks passed")
    
    return report


def process_yahoo_series_silver(bronze_data, series_name: str, parameters: dict[str, Any]):
    """
    Process Bronze layer data to Silver layer with enhanced features
    
    Args:
        bronze_data: Bronze layer DataFrame (Spark DataFrame from Bronze layer)
        series_name: Name of the series being processed
        parameters: Yahoo Finance configuration parameters
        
    Returns:
        Spark DataFrame for Silver layer
    """
    logger.info(f"🔄 Processing {series_name} data to Silver layer")
    
    try:
        # Check if we have data
        if hasattr(bronze_data, 'isEmpty') and bronze_data.isEmpty():
            logger.warning(f"⚠️ Empty Bronze DataFrame for {series_name}")
            spark = SparkSession.getActiveSession() or SparkSession.builder.appName("yahoo_silver").getOrCreate()
            return spark.createDataFrame([], schema="")
        
        # Convert Spark DataFrame to Pandas for processing
        if hasattr(bronze_data, 'toPandas'):
            df = bronze_data.toPandas()
        else:
            df = bronze_data.copy()
        
        if df.empty:
            logger.warning(f"⚠️ Empty Bronze DataFrame for {series_name}")
            spark = SparkSession.getActiveSession() or SparkSession.builder.appName("yahoo_silver").getOrCreate()
            return spark.createDataFrame([], schema="")
        
        # Sort by date to ensure proper calculation
        df = df.sort_values("date").reset_index(drop=True)
        
        # Calculate technical indicators
        df["rate_ma_7"] = df["rate"].rolling(window=7, min_periods=1).mean()
        df["rate_ma_30"] = df["rate"].rolling(window=30, min_periods=1).mean()
        df["rate_std_30"] = df["rate"].rolling(window=30, min_periods=1).std()
        
        # Calculate returns
        df["daily_return"] = df["rate"].pct_change()
        df["daily_return_abs"] = df["daily_return"].abs()
        
        # Calculate volatility (30-day rolling)
        df["volatility_30d"] = df["daily_return"].rolling(window=30, min_periods=5).std() * (252 ** 0.5)  # Annualized
        
        # Price momentum indicators
        df["price_change_1d"] = df["rate"].diff(1)
        df["price_change_7d"] = df["rate"].diff(7)
        df["price_change_30d"] = df["rate"].diff(30)
        
        # Relative strength (compared to 30-day average)
        df["relative_strength"] = df["rate"] / df["rate_ma_30"] - 1
        
        # Add silver layer metadata
        df["silver_processed_at"] = datetime.now().isoformat()
        df["silver_features_count"] = 8  # Number of calculated features
        
        # Clean up columns order
        feature_cols = [
            "date", "rate", "series", "source",
            "rate_ma_7", "rate_ma_30", "rate_std_30",
            "daily_return", "daily_return_abs", "volatility_30d",
            "price_change_1d", "price_change_7d", "price_change_30d",
            "relative_strength",
            "bronze_ingested_at", "silver_processed_at"
        ]
        
        # Keep only existing columns
        df = df[[col for col in feature_cols if col in df.columns]]
        
        logger.info(f"✅ Processed {len(df)} records to Silver layer for {series_name}")
        
        # Convert to Spark DataFrame
        spark = SparkSession.getActiveSession() or SparkSession.builder.appName("yahoo_silver").getOrCreate()
        spark_df = spark.createDataFrame(df)
        
        return spark_df
        
    except Exception as e:
        logger.error(f"❌ Error processing {series_name} to Silver layer: {e}")
        spark = SparkSession.getActiveSession() or SparkSession.builder.appName("yahoo_silver").getOrCreate()
        return spark.createDataFrame([], schema="")


def create_yahoo_catalog_entry(series_name: str, parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Create data catalog entry for Yahoo Finance series
    
    Args:
        series_name: Name of the series
        parameters: Yahoo Finance configuration parameters
        
    Returns:
        Catalog entry dictionary
    """
    series_config = parameters.get("yahoo_finance", {}).get("series", {}).get(series_name, {})
    
    return {
        "dataset_name": f"yahoo_{series_name}",
        "source": "Yahoo Finance",
        "description": series_config.get("description", f"Yahoo Finance {series_name} series"),
        "symbol": series_config.get("symbol", ""),
        "currency": series_config.get("currency", ""),
        "category": series_config.get("category", ""),
        "layers": {
            "raw": f"yahoo_{series_name}_raw",
            "bronze": f"yahoo_{series_name}_bronze",
            "silver": f"yahoo_{series_name}_silver"
        },
        "update_frequency": "daily",
        "data_quality_checks": True,
        "created_at": datetime.now().isoformat(),
        "last_updated": datetime.now().isoformat()
    }
