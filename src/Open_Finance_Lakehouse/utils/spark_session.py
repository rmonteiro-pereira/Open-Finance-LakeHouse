import os
import sys
import logging
from functools import lru_cache

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

@lru_cache(maxsize=1)
def get_spark_session(app_name: str = "OpenFinanceLakehouse") -> SparkSession:
    """
    Retorna uma SparkSession singleton usando cache interno.
    Configuração mínima para teste.
    """

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.executor.memory", "16g")
        .config("spark.driver.memory", "16g")
        .getOrCreate()
    )
    
    # Set basic log level
    spark.sparkContext.setLogLevel("ERROR")
    
    # Add basic Python logging suppression
    logging.getLogger('py4j').setLevel(logging.ERROR)
    logging.getLogger('pyspark').setLevel(logging.ERROR)
    logging.getLogger('org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec').setLevel(logging.OFF)
    logging.getLogger('org.apache.spark.sql.execution').setLevel(logging.ERROR)
    
    # Suppress Hadoop metrics warnings
    logging.getLogger('org.apache.hadoop.metrics2.impl.MetricsSystemImpl').setLevel(logging.OFF)
    logging.getLogger('org.apache.hadoop.metrics2').setLevel(logging.ERROR)
    
    return spark
