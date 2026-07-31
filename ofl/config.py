"""Runtime configuration, sourced from the environment (12-factor).

Replaces Kedro's ``conf/base`` + ``conf/local``. No secrets in code: MinIO
credentials and endpoints come from env vars / sealed secrets only.
"""

from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# MinIO's own published out-of-the-box root credentials. They are a *placeholder*
# so that `docker run minio` and a scratch localhost stack work with no setup — not
# a credential for any deployment of this project. Anything that is not loopback
# must supply MINIO_USER / MINIO_PASSWORD from a real secret; see the validator
# below, which refuses to start otherwise.
_VENDOR_DEFAULT_CREDENTIAL = "minioadmin"
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})


class Settings(BaseSettings):
    """Object-store + lakehouse settings, read from env (prefix-free)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    minio_endpoint: str = Field("http://localhost:9000", alias="MINIO_ENDPOINT")
    minio_user: str = Field(_VENDOR_DEFAULT_CREDENTIAL, alias="MINIO_USER")
    minio_password: str = Field(_VENDOR_DEFAULT_CREDENTIAL, alias="MINIO_PASSWORD")
    aws_region: str = Field("us-east-1", alias="AWS_REGION")

    bucket: str = Field("lakehouse", alias="LAKEHOUSE_BUCKET")
    registry_path: str = Field("sources/registry.yml", alias="OFL_REGISTRY")

    # When the Spark image already bakes Delta/S3A jars (the cluster image does),
    # skip Ivy resolution at session-build time.
    spark_jars_packaged: bool = Field(False, alias="OFL_SPARK_JARS_PACKAGED")

    # Driver heap for the silver lane. The conform MERGEs re-read each fact's full
    # bronze every run (not just the daily delta), so the largest fact
    # (fact_derivatives_quote, ~1.6M rows and growing) drives the requirement; the
    # 1g pyspark default OOMs on it. Keep within the pod memory limit.
    spark_driver_memory: str = Field("4g", alias="OFL_SPARK_DRIVER_MEMORY")

    # Streaming lane. Landing files, bronze Delta and checkpoints live on the local
    # filesystem: the production ``lakehouse`` bucket is read-only for this lane, and
    # the zero-cost "live" tier (M3+) repoints this root at object storage instead.
    streaming_root: str = Field("data/streaming", alias="OFL_STREAMING_ROOT")

    # Backpressure for the file source: how many landing files a micro-batch may
    # claim. Bounds batch size (and driver memory) when the job restarts on a
    # backlog rather than tailing a live producer.
    streaming_max_files_per_trigger: int = Field(64, alias="OFL_STREAM_MAX_FILES_PER_TRIGGER")

    @model_validator(mode="after")
    def _reject_default_credentials_off_loopback(self) -> Settings:
        """Fail fast if the vendor default credentials would leave the machine.

        The defaults exist so a scratch localhost stack needs zero setup. Sending
        them to a remote endpoint means either (a) the real credentials were never
        wired up, or (b) the remote MinIO is still on its factory root user — and
        the second case is a finding, not a configuration. Either way, refusing at
        construction time is better than a 403 three lanes downstream.
        """
        host = (urlparse(self.minio_endpoint).hostname or "").lower()
        uses_defaults = _VENDOR_DEFAULT_CREDENTIAL in {self.minio_user, self.minio_password}
        if uses_defaults and host not in _LOOPBACK_HOSTS:
            raise ValueError(
                f"MINIO_ENDPOINT points at {host!r}, but MINIO_USER/MINIO_PASSWORD are "
                "still MinIO's published factory defaults. Set both from a real secret "
                "(sealed secret / OpenBao / env), or point MINIO_ENDPOINT at localhost "
                "for a scratch stack."
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
