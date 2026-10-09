"""Where archived documents live: files keyed by content hash, plus a manifest per manager.

Layout (same in S3 and in the local backend used by tests and dry development)::

    letters/files/<manager>/<sha256>.pdf
    letters/manifest/<manager>.jsonl      one line per URL ever seen

The manifest is the memory of a run: a URL already in it is not fetched again, which is
what makes a collection resumable and a monthly run cheap.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Protocol

PREFIX = "letters"


class Backend(Protocol):
    def read(self, key: str) -> bytes | None: ...
    def write(self, key: str, data: bytes, content_type: str) -> None: ...
    def exists(self, key: str) -> bool: ...


class LocalBackend:
    def __init__(self, root: Path | str):
        self.root = Path(root)

    def read(self, key: str) -> bytes | None:
        path = self.root / key
        return path.read_bytes() if path.exists() else None

    def write(self, key: str, data: bytes, content_type: str) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def exists(self, key: str) -> bool:
        return (self.root / key).exists()


class S3Backend:
    def __init__(self, bucket: str):
        import boto3
        from botocore.exceptions import ClientError

        from ofl.config import get_settings

        s = get_settings()
        self._missing = ClientError
        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url=s.minio_endpoint,
            aws_access_key_id=s.minio_user,
            aws_secret_access_key=s.minio_password,
            region_name=s.aws_region,
        )

    def read(self, key: str) -> bytes | None:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except self._missing as exc:
            if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
                return None
            raise

    def write(self, key: str, data: bytes, content_type: str) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except self._missing as exc:
            if exc.response["Error"]["Code"] in {"NoSuchKey", "404", "NotFound"}:
                return False
            raise


def default_backend() -> Backend:
    """``DOCUMENTS_DIR`` selects a local folder; otherwise the ``DOCUMENTS_BUCKET`` bucket."""
    local = os.getenv("DOCUMENTS_DIR")
    if local:
        return LocalBackend(local)
    return S3Backend(os.getenv("DOCUMENTS_BUCKET", "documents"))


def file_key(manager: str, sha256: str) -> str:
    return f"{PREFIX}/files/{manager}/{sha256}.pdf"


def manifest_key(manager: str) -> str:
    return f"{PREFIX}/manifest/{manager}.jsonl"


def load_manifest(backend: Backend, manager: str) -> dict[str, dict]:
    """URL -> manifest row."""
    raw = backend.read(manifest_key(manager))
    if not raw:
        return {}
    rows = (json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip())
    return {row["url"]: row for row in rows}


def save_manifest(backend: Backend, manager: str, rows: dict[str, dict]) -> None:
    body = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows.values())
    backend.write(manifest_key(manager), body.encode("utf-8"), "application/x-ndjson")
