"""Spark session factory — Delta + S3A (MinIO) configured from env.

Used only by the silver lane (bronze -> silver conform/MERGE). Extras: install
with ``pip install -e .[spark]``. On the cluster image the Delta/S3A jars are
baked in; set ``OFL_SPARK_JARS_PACKAGED=1`` to skip Ivy resolution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ofl.config import get_settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "ofl-silver") -> "SparkSession":
    from pyspark.sql import SparkSession

    s = get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", s.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s.minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", s.minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # single-node: keep shuffle small; this data is tiny.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    )

    if s.spark_jars_packaged:
        return builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(builder).getOrCreate()
