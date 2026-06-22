"""Lakehouse path + object-store helpers (MinIO / S3 via delta-rs).

Layout: ``s3://{bucket}/{layer}/{...}`` where layer is raw|bronze|silver|gold.
``deltalake`` (delta-rs) and Polars both consume ``delta_storage_options()``.
"""

from __future__ import annotations

from ofl.config import get_settings


def _layer(layer: str) -> str:
    return f"s3://{get_settings().bucket}/{layer}"


def raw_uri(fact: str, series_key: str) -> str:
    return f"{_layer('raw')}/{fact}/{series_key}"


def bronze_uri(fact: str, series_key: str) -> str:
    return f"{_layer('bronze')}/{fact}/{series_key}"


def silver_uri(table: str) -> str:
    return f"{_layer('silver')}/{table}"


def gold_uri(table: str) -> str:
    return f"{_layer('gold')}/{table}"


def to_spark_path(uri: str) -> str:
    """Spark/Hadoop addresses MinIO via the ``s3a://`` scheme; delta-rs uses ``s3://``."""
    return uri.replace("s3://", "s3a://", 1)


def delta_storage_options() -> dict[str, str]:
    """Storage options for delta-rs / Polars against MinIO."""
    s = get_settings()
    return {
        "AWS_ENDPOINT_URL": s.minio_endpoint,
        "AWS_ACCESS_KEY_ID": s.minio_user,
        "AWS_SECRET_ACCESS_KEY": s.minio_password,
        "AWS_REGION": s.aws_region,
        "AWS_ALLOW_HTTP": "true",
        # MinIO is a single-writer here; safe-rename guard is unnecessary and
        # blocks writes without an external lock provider.
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
