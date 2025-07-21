"""
Common nodes for all BACEN series pipelines
"""
import os
import time
import json
import logging
from functools import wraps
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, year, month, first, last, avg, count, stddev, lit, current_timestamp, min, max
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
MAX_REASONABLE_RATE = 100.0  # Maximum reasonable rate for validation


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
    Ensure MinIO bucket exists, create if it doesn't
    """
    if not BOTO3_AVAILABLE:
        logger.warning("[BUCKET] boto3 not available, skipping bucket creation")
        return
    
    try:
        # MinIO connection using environment variables
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
            aws_access_key_id=os.getenv('MINIO_USER', 'minioadmin'),
            aws_secret_access_key=os.getenv('MINIO_PASSWORD', 'minioadmin')
        )
        
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"[BUCKET] MinIO bucket '{bucket_name}' already exists")
        
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == HTTP_NOT_FOUND:
            # Bucket doesn't exist, create it
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"[BUCKET] Created MinIO bucket '{bucket_name}'")
            except ClientError as create_error:
                logger.error(f"[BUCKET] Failed to create bucket '{bucket_name}': {create_error}")
                raise
        else:
            logger.error(f"[BUCKET] Error accessing bucket '{bucket_name}': {e}")
            raise


@timing_decorator
def ingest_bacen_raw(series_id: int, end_date: str = None) -> str:
    """
    Generic BACEN data ingestion node
    Fetch raw data from BACEN API and save as JSON format
    """
    logger.info(f"[RAW] Fetching raw BACEN data for series {series_id}")
    
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
def transform_raw_to_bronze_generic(raw_data: str, series_id: int) -> DataFrame:
    """
    Generic Bronze transformation for any BACEN series
    """
    logger.info(f"[BRONZE] Transforming raw data to bronze layer for series {series_id}")
    
    # Parse JSON string back to dict
    data_dict = json.loads(raw_data)
    
    # Ensure MinIO bucket exists
    ensure_minio_bucket("lakehouse")
    
    # Get Spark session
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No Spark session available")
    
    # Extract raw data
    raw_records = data_dict["data"]
    pandas_df = pd.DataFrame(raw_records)
    
    # Create bronze DataFrame with metadata
    bronze_df = spark.createDataFrame(pandas_df)
    
    # Add metadata columns
    bronze_df = bronze_df.withColumn("series_id", col("valor").cast("int") * 0 + series_id)
    bronze_df = bronze_df.withColumn("ingestion_timestamp", col("valor").cast("timestamp"))
    
    logger.info(f"[BRONZE] Created bronze layer with {bronze_df.count()} records")
    
    return bronze_df


@timing_decorator
def transform_bronze_to_silver_generic(bronze_df: DataFrame, series_id: int) -> DataFrame:
    """
    Generic Silver transformation for any BACEN series
    """
    logger.info(f"[SILVER] Transforming bronze data to silver layer for series {series_id}")
    
    # Convert data column to proper date format
    silver_df = bronze_df.withColumn("date", to_date(col("data"), "dd/MM/yyyy"))
    
    # Convert valor to numeric, handling potential null values
    silver_df = silver_df.withColumn("rate", col("valor").cast("double"))
    
    # Add derived columns
    silver_df = silver_df.withColumn("year", year(col("date")))
    silver_df = silver_df.withColumn("month", month(col("date")))
    
    # Select final columns
    silver_df = silver_df.select(
        "series_id",
        "date",
        "rate",
        "year",
        "month",
        "ingestion_timestamp"
    )
    
    logger.info(f"[SILVER] Silver transformation complete: {silver_df.count()} records")
    
    return silver_df


@timing_decorator
def validate_bacen_data_generic(silver_df: DataFrame, series_id: int, max_rate: float = 100.0) -> str:
    """
    Generic validation for any BACEN series
    """
    logger.info(f"[VALIDATE] Running data quality validation for series {series_id}...")
    
    # Basic validation checks
    total_records = silver_df.count()
    null_dates = silver_df.filter(col("date").isNull()).count()
    null_rates = silver_df.filter(col("rate").isNull()).count()
    
    # Series-specific validation
    invalid_rates = silver_df.filter(
        (col("rate") < 0) | (col("rate") > max_rate)
    ).count()
    
    # Create validation results
    validation_results = {
        "series_id": series_id,
        "total_records": total_records,
        "null_dates": null_dates,
        "null_rates": null_rates,
        "invalid_rates": invalid_rates,
        "validation_passed": null_dates == 0 and null_rates == 0 and invalid_rates == 0,
        "validated_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    logger.info(f"[RESULTS] Validation results: {total_records} total records, {null_dates} null dates, {null_rates} null rates, {invalid_rates} invalid rates")
    
    if validation_results["validation_passed"]:
        logger.info(f"[VALID] Data validation passed for series {series_id}")
    else:
        logger.warning(f"[INVALID] Data validation failed for series {series_id}")
    
    return json.dumps(validation_results, indent=2)


@timing_decorator
def aggregate_bacen_to_gold_generic(silver_df: DataFrame, series_name: str) -> DataFrame:
    """
    Generic Gold layer aggregation for any BACEN series
    Creates monthly aggregations for business intelligence
    """
    logger.info(f"[GOLD] Creating {series_name} gold layer aggregations...")
    
    # Group by year and month to create monthly aggregations
    gold_df = silver_df.groupBy("year", "month") \
        .agg(
            first("date").alias("month_start_date"),
            avg("rate").alias("avg_rate"),
            min("rate").alias("min_rate"),
            max("rate").alias("max_rate"),
            last("rate").alias("last_rate"),
            count("rate").alias("count_observations"),
            stddev("rate").alias("rate_volatility")
        ) \
        .withColumn("series_name", lit(series_name)) \
        .withColumn("created_at", current_timestamp()) \
        .orderBy("year", "month")
    
    monthly_count = gold_df.count()
    logger.info(f"[GOLD] {series_name} gold aggregation complete: {monthly_count} monthly aggregations")
    
    return gold_df
