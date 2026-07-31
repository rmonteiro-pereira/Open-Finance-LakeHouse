"""Lakehouse path + object-store helpers (MinIO / S3 via delta-rs).

Layout: ``s3://{bucket}/{layer}/{...}`` where layer is raw|bronze|silver|gold.
``deltalake`` (delta-rs) and Polars both consume ``delta_storage_options()``.
"""

from __future__ import annotations

from pathlib import Path

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


def streaming_dir(*parts: str) -> Path:
    """A directory under the streaming root (``OFL_STREAMING_ROOT``, local by default).

    The streaming lane deliberately does *not* share the batch lane's object-store
    layout: its landing files and checkpoints are hot, high-churn state, and the
    production bucket is read-only for it. See ``ofl.streaming.paths``.

    The root must be a **local filesystem path**. The lane's producer relies on
    atomic renames and the mart is a DuckDB file, so an object-store root
    (``s3://…``) is not supported yet — and ``Path("s3://…")`` would silently
    mangle the URI into a local directory literally named ``s3:``. Refuse loudly
    instead of corrupting the exactly-once story; the repoint is a tracked
    roadmap item (README §Roadmap, ``docs/STREAMING.md``).
    """
    root = get_settings().streaming_root
    if "://" in root:
        raise NotImplementedError(
            f"OFL_STREAMING_ROOT={root!r} looks like an object-store URI, but the "
            "streaming lane only supports local filesystem roots today (atomic "
            "landing renames + a DuckDB mart file). Point it at a local directory, "
            "or see the roadmap for the object-store repoint."
        )
    return Path(root).joinpath(*parts)


def to_local_uri(path: Path | str) -> str:
    """Spark addresses local paths by URI — mandatory on Windows (``file:///E:/...``)."""
    return Path(path).resolve().as_uri()


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
