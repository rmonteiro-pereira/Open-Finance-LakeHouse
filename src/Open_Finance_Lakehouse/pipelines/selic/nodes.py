"""
This is a boilerplate pipeline 'selic'
generated using Kedro 0.19.12
"""
import os
import time
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
def ingest_selic_data(series_id: int, end_date: str = None) -> pd.DataFrame:
    """
    Stage 2: Ingestão - Fetch SELIC data from BACEN API
    Returns pandas DataFrame that will be converted to Spark by Kedro
    """
    logger.info(f"[FETCH] Fetching SELIC data for series {series_id}")
    result = fetch_bacen_series(series_id, end_date)
    logger.info(f"[RETRIEVED] Retrieved {len(result)} records from BACEN API")
    return result


@timing_decorator
def transform_selic_to_silver(bronze_selic: pd.DataFrame) -> DataFrame:
    """
    Stage 3: Transformação Silver - Clean and standardize SELIC data
    Convert pandas DataFrame to Spark and apply transformations with enhanced error handling
    """
    logger.info(f"[TRANSFORM] Starting silver transformation with {len(bronze_selic)} records")
    
    # Ensure MinIO bucket exists
    ensure_minio_bucket("lakehouse")

    # Get Spark session - Use existing session managed by Kedro
    spark = SparkSession.getActiveSession()
    if spark is None:
        logger.error("❌ No active Spark session found. Ensure Kedro is managing Spark sessions properly.")
        raise RuntimeError("No Spark session available")

    logger.info(f"[CONVERT] Converting {len(bronze_selic)} pandas records to Spark...")

    # Convert pandas to Spark directly - no chunking for small datasets
    try:
        schema = StructType([
            StructField("data", StringType(), True),
            StructField("valor", StringType(), True)
        ])
        spark_df = spark.createDataFrame(bronze_selic, schema=schema)
        
        # Coalesce to single partition for small datasets to improve performance
        spark_df = spark_df.coalesce(1)
        
        record_count = spark_df.count()
        logger.info(f"[CONVERTED] Successfully converted to Spark DataFrame with {record_count} records")

    except Exception as e:
        logger.error(f"❌ Error in pandas to Spark conversion: {e}")
        raise

    # Apply silver transformations with better error handling
    logger.info("[TRANSFORM] Applying silver transformations...")
    try:
        silver_df = spark_df.select(
            to_date(col("data"), "dd/MM/yyyy").alias("date"),  # BACEN uses DD/MM/YYYY format
            col("valor").cast("double").alias("selic_rate")
        ).filter(
            col("selic_rate").isNotNull() &
            col("date").isNotNull()
        ).orderBy("date")

        # Coalesce to single partition for small datasets and cache for performance
        silver_df = silver_df.coalesce(1).cache()
        record_count = silver_df.count()
        logger.info(f"[COMPLETE] Silver transformation complete: {record_count} records")

        return silver_df

    except Exception as e:
        logger.error(f"❌ Silver transformation failed: {e}")
        raise


@timing_decorator
def validate_selic_data(silver_selic: DataFrame) -> dict[str, Any]:
    """
    Stage 4: Validação - Data quality checks for SELIC
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
        }
    }
    
    logger.info(f"[RESULTS] Validation results: {validation_results['total_records']} total records, "
                f"{validation_results['null_dates']} null dates, "
                f"{validation_results['null_rates']} null rates")
    
    # Basic validation rules
    is_valid = (
        validation_results["null_dates"] == 0 and
        validation_results["null_rates"] == 0 and
        validation_results["negative_rates"] == 0 and
        validation_results["total_records"] > 0
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
