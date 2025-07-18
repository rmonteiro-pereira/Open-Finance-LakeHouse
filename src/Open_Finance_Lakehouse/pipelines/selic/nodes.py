"""
This is a boilerplate pipeline 'selic'
generated using Kedro 0.19.12
"""
import os
import time
import json
import logging
from typing import Any
from functools import wraps
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, year, month, avg, min, max, stddev
from pyspark.sql.types import StructType, StructField, StringType
from ...utils.bacen_api import fetch_bacen_series

try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

# Set up logging
logger = logging.getLogger(__name__)

# Constants
HTTP_NOT_FOUND = 404
MAX_REASONABLE_SELIC_RATE = 100.0  # Maximum reasonable SELIC rate for validation


def timing_decorator(func):
    """Decorator to time function execution"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"[START] Starting {func.__name__}")
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"[SUCCESS] Completed {func.__name__} in {execution_time:.2f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"[ERROR] Failed {func.__name__} after {execution_time:.2f}s: {e}")
            raise
    return wrapper


def ensure_minio_bucket(bucket_name: str = "lakehouse") -> None:
    """
    Ensure the MinIO bucket exists before Spark operations
    """
    if not BOTO3_AVAILABLE:
        logger.warning("⚠️  boto3 not available for bucket creation")
        return
        
    try:
        # Get MinIO credentials from environment
        endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        access_key = os.getenv("MINIO_USER")
        secret_key = os.getenv("MINIO_PASSWORD")
        
        if not access_key or not secret_key:
            logger.warning("⚠️  MinIO credentials not found in environment variables")
            return
            
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"[BUCKET] MinIO bucket '{bucket_name}' already exists")
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == HTTP_NOT_FOUND:
                logger.info(f"[CREATE] Creating MinIO bucket '{bucket_name}'...")
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"[BUCKET] MinIO bucket '{bucket_name}' created successfully")
            else:
                logger.error(f"⚠️  Error checking MinIO bucket: {e}")
                
    except Exception as e:
        logger.error(f"⚠️  Error ensuring MinIO bucket: {e}")


@timing_decorator
def ingest_selic_raw(series_id: int, end_date: str = None) -> str:
    """
    Stage 1: Raw Data Ingestion - Fetch raw SELIC data from BACEN API
    Save as-is JSON format to preserve original structure
    """
    logger.info(f"[RAW] Fetching raw SELIC data for series {series_id}")
    
    # Fetch raw data from BACEN API
    pandas_result = fetch_bacen_series(series_id, end_date)
    
    # Convert to dictionary format for JSON storage
    raw_data = {
        "series_id": series_id,
        "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_records": len(pandas_result),
        "data": pandas_result.to_dict(orient="records")
    }
    
    logger.info(f"[RAW] Retrieved {len(pandas_result)} raw records from BACEN API")
    
    # Return as JSON string for TextDataset compatibility
    return json.dumps(raw_data, indent=2)


@timing_decorator
def transform_raw_to_bronze(raw_selic: str) -> DataFrame:
    """
    Stage 2: Raw to Bronze - Apply basic schema and store in Delta format
    Bronze layer contains raw data with basic validation
    """
    logger.info("[BRONZE] Transforming raw data to bronze layer")
    
    # Parse JSON string back to dict
    raw_data = json.loads(raw_selic)
    
    # Ensure MinIO bucket exists
    ensure_minio_bucket("lakehouse")
    
    # Get Spark session
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No Spark session available")
    
    # Extract raw data
    raw_records = raw_data["data"]
    pandas_df = pd.DataFrame(raw_records)
    
    # Create bronze DataFrame with metadata
    schema = StructType([
        StructField("data", StringType(), True),
        StructField("valor", StringType(), True),
        StructField("series_id", StringType(), True),
        StructField("ingested_at", StringType(), True)
    ])
    
    # Add metadata columns
    pandas_df['series_id'] = str(raw_data["series_id"])
    pandas_df['ingested_at'] = raw_data["fetched_at"]
    
    bronze_df = spark.createDataFrame(pandas_df, schema=schema)
    bronze_df = bronze_df.coalesce(1)
    
    record_count = bronze_df.count()
    logger.info(f"[BRONZE] Created bronze layer with {record_count} records")
    
    return bronze_df


@timing_decorator
def transform_bronze_to_silver(bronze_selic: DataFrame) -> DataFrame:
    """
    Stage 3: Bronze to Silver - Clean and standardize SELIC data
    Silver layer contains validated and cleaned data
    """
    logger.info("[SILVER] Transforming bronze data to silver layer")
    
    # Apply silver transformations with validation
    try:
        silver_df = bronze_selic.select(
            to_date(col("data"), "dd/MM/yyyy").alias("date"),  # BACEN uses DD/MM/YYYY format
            col("valor").cast("double").alias("selic_rate"),
            col("series_id"),
            col("ingested_at")
        ).filter(
            col("selic_rate").isNotNull() &
            col("date").isNotNull() &
            (col("selic_rate") >= 0)  # Basic business rule validation
        ).orderBy("date")

        # Coalesce and cache for performance
        silver_df = silver_df.coalesce(1).cache()
        record_count = silver_df.count()
        logger.info(f"[SILVER] Silver transformation complete: {record_count} records")

        return silver_df

    except Exception as e:
        logger.error(f"❌ Silver transformation failed: {e}")
        raise


@timing_decorator
def validate_selic_data(silver_selic: DataFrame) -> dict[str, Any]:
    """
    Stage 4: Data Validation - Data quality checks for SELIC silver layer
    """
    logger.info("[VALIDATE] Running data quality validation...")
    
    validation_results = {
        "total_records": silver_selic.count(),
        "null_dates": silver_selic.filter(col("date").isNull()).count(),
        "null_rates": silver_selic.filter(col("selic_rate").isNull()).count(),
        "negative_rates": silver_selic.filter(col("selic_rate") < 0).count(),
        "date_range": {
            "min_date": str(silver_selic.agg({"date": "min"}).collect()[0][0]),
            "max_date": str(silver_selic.agg({"date": "max"}).collect()[0][0])
        },
        "rate_stats": {
            "min_rate": float(silver_selic.agg({"selic_rate": "min"}).collect()[0][0]),
            "max_rate": float(silver_selic.agg({"selic_rate": "max"}).collect()[0][0]),
            "avg_rate": float(silver_selic.agg({"selic_rate": "avg"}).collect()[0][0])
        }
    }
    
    logger.info(f"[RESULTS] Validation results: {validation_results['total_records']} total records, "
                f"{validation_results['null_dates']} null dates, "
                f"{validation_results['null_rates']} null rates")
    
    # Enhanced validation rules
    is_valid = (
        validation_results["null_dates"] == 0 and
        validation_results["null_rates"] == 0 and
        validation_results["negative_rates"] == 0 and
        validation_results["total_records"] > 0 and
        validation_results["rate_stats"]["min_rate"] >= 0 and
        validation_results["rate_stats"]["max_rate"] <= MAX_REASONABLE_SELIC_RATE
    )
    
    validation_results["is_valid"] = is_valid
    
    if is_valid:
        logger.info("[VALID] Data validation passed")
    else:
        logger.warning("⚠️ Data validation issues found")
    
    return validation_results


@timing_decorator
def aggregate_selic_to_gold(silver_selic: DataFrame) -> DataFrame:
    """
    Stage 5: Agregação Gold - Create SELIC KPIs and derived metrics
    """
    logger.info("[GOLD] Creating gold layer aggregations...")
    
    # Monthly aggregations
    monthly_selic = silver_selic.groupBy(
        year("date").alias("year"),
        month("date").alias("month")
    ).agg(
        avg("selic_rate").alias("avg_selic_rate"),
        min("selic_rate").alias("min_selic_rate"),
        max("selic_rate").alias("max_selic_rate"),
        stddev("selic_rate").alias("std_selic_rate")
    )
    
    # Coalesce for performance and cache
    monthly_selic = monthly_selic.coalesce(1).cache()
    record_count = monthly_selic.count()
    logger.info(f"[GOLD] Gold aggregation complete: {record_count} monthly aggregations")
    
    return monthly_selic


def convert_pandas_to_spark(pandas_df: pd.DataFrame) -> DataFrame:
    """
    Convert Pandas DataFrame to Spark DataFrame for lakehouse processing
    Ensures MinIO bucket exists before returning the DataFrame
    """
    # Ensure MinIO bucket exists before Spark operations
    ensure_minio_bucket("lakehouse")
    
    spark = SparkSession.getActiveSession()
    
    # Add explicit schema to avoid inference issues
    schema = StructType([
        StructField("data", StringType(), True),
        StructField("valor", StringType(), True)
    ])
    
    return spark.createDataFrame(pandas_df, schema=schema)
