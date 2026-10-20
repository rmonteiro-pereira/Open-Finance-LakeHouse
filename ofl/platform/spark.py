"""Spark session factory — Delta + S3A (S3-compatible storage) configured from env.

Used only by the silver lane (bronze -> silver conform/MERGE). Extras: install
with ``pip install -e .[spark]``. On the cluster image the Delta/S3A jars are
baked in; set ``OFL_SPARK_JARS_PACKAGED=1`` to skip Ivy resolution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ofl.config import get_settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def build_spark_session(
    app_name: str = "ofl-silver", extra_conf: dict[str, str] | None = None
) -> "SparkSession":
    """Build the shared Delta+S3A session; ``extra_conf`` adds lane-specific keys."""
    from pyspark.sql import SparkSession

    s = get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        # pyspark reads driver.memory at JVM launch (local mode) — the 1g default
        # OOMs on the largest conform MERGE (fact_derivatives_quote).
        .config("spark.driver.memory", s.spark_driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", s.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s.s3_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", s.s3_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # The AWS SDK guesses the signing region from the hostname; a service name
        # with an ".s3." label (versitygw.s3.svc...) yields a wrong region and a 400.
        .config("spark.hadoop.fs.s3a.endpoint.region", s.aws_region)
        # POSIX-backed stores refuse to delete a "dir/" marker that still holds objects,
        # and S3A retries each refusal with backoff. Keeping markers is valid on any S3.
        .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
        # single-node: keep shuffle small; this data is tiny.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    )

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    if s.spark_jars_packaged:
        return builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(builder).getOrCreate()
