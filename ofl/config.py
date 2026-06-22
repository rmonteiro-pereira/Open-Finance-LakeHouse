"""Runtime configuration, sourced from the environment (12-factor).

Replaces Kedro's ``conf/base`` + ``conf/local``. No secrets in code: MinIO
credentials and endpoints come from env vars / sealed secrets only.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Object-store + lakehouse settings, read from env (prefix-free)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    minio_endpoint: str = Field("http://localhost:9000", alias="MINIO_ENDPOINT")
    minio_user: str = Field("minioadmin", alias="MINIO_USER")
    minio_password: str = Field("minioadmin", alias="MINIO_PASSWORD")
    aws_region: str = Field("us-east-1", alias="AWS_REGION")

    bucket: str = Field("lakehouse", alias="LAKEHOUSE_BUCKET")
    registry_path: str = Field("sources/registry.yml", alias="OFL_REGISTRY")

    # When the Spark image already bakes Delta/S3A jars (the cluster image does),
    # skip Ivy resolution at session-build time.
    spark_jars_packaged: bool = Field(False, alias="OFL_SPARK_JARS_PACKAGED")


@lru_cache
def get_settings() -> Settings:
    return Settings()
