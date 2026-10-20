"""Separate settings for the private research application."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ResearchSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OFL_RESEARCH_", extra="ignore")
    database_url: str = "postgresql:///ofl_research"
    root: Path = Path("data/research")
    # Empty means the API fails closed. Secrets belong in environment/Secrets.
    gateway_token: str = ""
    roles: str = ""  # JSON mapping verified gateway identities to reader/reviewer/admin
    service_tokens: str = "{}"  # JSON principal -> read-only bearer token (minimum 32 characters)
    api_url: str = "http://127.0.0.1:8081"
    api_token: str = ""  # MCP client credential; never distribute the gateway secret
    origin: str = "http://localhost:3000"
    batch_rows: int = Field(50_000, ge=1, le=100_000)
    publication_days: int = Field(1, ge=1, le=31)
    max_upload_bytes: int = Field(2 * 1024**3, ge=1)
    max_uncompressed_bytes: int = Field(32 * 1024**3, ge=1)
    minimum_free_bytes: int = Field(1024**3, ge=0)
    objects_bucket: str = ""  # private S3 originals; empty selects local development
    embedding_model: str = "BAAI/bge-m3"
    embedding_url: str = ""  # private Ollama endpoint; empty uses local SentenceTransformer
    embedding_revision: str = ""  # pin a model commit before embedding a corpus
    embedding_batch_size: int = Field(1, ge=1, le=32)  # remote GPU opt-in; CPU-safe default
    embedding_threads: int = Field(1, ge=1, le=32)  # request-level Ollama CPU budget
    embedding_dimensions: int = Field(1024, ge=1, le=2000)
    approximate_search: bool = False  # enable only after a filtered-recall pilot
    lease_seconds: int = Field(300, ge=30)
    library_bucket: str = "documents"
    snapshot_bucket: str = "ofl-public"
    bridge_object_limit: int = Field(10000, ge=1, le=100000)
    bridge_metadata_bytes: int = Field(8 * 1024**2, ge=1024)
    bridge_asset_bytes: int = Field(64 * 1024**2, ge=1024)
    airflow_url: str = ""
    airflow_token: str = ""
