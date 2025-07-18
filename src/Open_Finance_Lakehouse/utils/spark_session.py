import os
import sys
from functools import lru_cache

from dotenv import load_dotenv
from pyspark.sql import SparkSession
import logging

load_dotenv()

@lru_cache(maxsize=1)
def get_spark_session(app_name: str = "OpenFinanceLakehouse") -> SparkSession:
    """
    Retorna uma SparkSession singleton usando cache interno.
    Não usa variáveis globais.
    """

    logging.getLogger('py4j').setLevel(logging.ERROR)
    logging.getLogger('org.apache.ivy').setLevel(logging.ERROR)
    logging.getLogger('org.apache.spark').setLevel(logging.ERROR)
    logging.getLogger('pyspark').setLevel(logging.ERROR)
    logging.getLogger('spark').setLevel(logging.ERROR)
    logging.getLogger('org.sparkproject').setLevel(logging.ERROR)
    logging.getLogger('org.apache.hadoop').setLevel(logging.ERROR)
    logging.getLogger('org.apache.parquet').setLevel(logging.ERROR)
    logging.getLogger('parquet').setLevel(logging.ERROR)

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    # Set environment variables to suppress Spark command output
    os.environ["SPARK_PRINT_LAUNCH_COMMAND"] = "0"
    os.environ["SPARK_LAUNCHER_VERBOSE"] = "0"
    os.environ["SPARK_SUBMIT_OPTS"] = os.environ.get("SPARK_SUBMIT_OPTS", "") + " -Dspark.launcher.verbose=0"
    
    # Try to suppress at the Java system property level
    java_opts = [
        "-Dspark.launcher.verbose=false",
        "-Dspark.submit.quiet=true",
        "-Dspark.launcher.quiet=true"
    ]
    
    existing_opts = os.environ.get("SPARK_SUBMIT_OPTS", "")
    for opt in java_opts:
        if opt not in existing_opts:
            existing_opts += f" {opt}"
    
    os.environ["SPARK_SUBMIT_OPTS"] = existing_opts
    
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
        .config("spark.launcher.quiet", "true")
        .config("spark.submit.quiet", "true")
        .config("spark.sql.adaptive.logLevel", "ERROR")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.eventLog.enabled", "false")
        .config("spark.python.daemon.log", "false")
        .config("spark.python.worker.log", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.adaptive.skewJoin.enabled", "false")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:conf/log4j.properties -Dlog4j.rootLogger=ERROR,console")
        .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:conf/log4j.properties -Dlog4j.rootLogger=ERROR,console")
        .getOrCreate()
    )
    
    # Set Spark log level to suppress verbose output
    spark.sparkContext.setLogLevel("ERROR")
    
    # Also set the root logger to ERROR through Java system properties
    spark.sparkContext._jsc.sc().setLogLevel("ERROR")
    
    return spark
