"""Streaming immutable files on a private volume; no filename-derived paths."""

import hashlib
import os
import shutil
import tempfile
from pathlib import Path


class Store:
    def __init__(self, root: Path, minimum_free_bytes=0, mirror=None):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.minimum_free_bytes = minimum_free_bytes
        self.mirror = mirror

    @classmethod
    def from_settings(cls, settings):
        mirror = None
        if settings.objects_bucket:
            import boto3

            from ofl.config import get_settings

            config = get_settings()
            client = boto3.client(
                "s3",
                endpoint_url=config.minio_endpoint,
                aws_access_key_id=config.minio_user,
                aws_secret_access_key=config.minio_password,
                region_name=config.aws_region,
            )
            mirror = (client, settings.objects_bucket)
        return cls(settings.root, settings.minimum_free_bytes, mirror)

    def path(self, key: str) -> Path:
        p = (self.root / key).resolve()
        if not p.is_relative_to(self.root) or p == self.root:
            raise ValueError("invalid storage key")
        if self.mirror and key.startswith("originals/") and not p.exists():
            self.check_space()
            p.parent.mkdir(parents=True, exist_ok=True)
            client, bucket = self.mirror
            metadata = client.head_object(Bucket=bucket, Key="research/" + key)
            self.check_space(metadata["ContentLength"])
            temporary = p.with_suffix("." + __import__("uuid").uuid4().hex + ".tmp")
            try:
                client.download_file(bucket, "research/" + key, str(temporary))
                with temporary.open("rb") as stream:
                    if hashlib.file_digest(stream, "sha256").hexdigest() != p.name:
                        raise ValueError("object-store original checksum mismatch")
                os.replace(temporary, p)
            finally:
                temporary.unlink(missing_ok=True)
        return p

    def check_space(self, needed=0):
        usage = shutil.disk_usage(self.root)
        if usage.free - needed < max(self.minimum_free_bytes, int(usage.total * 0.25)):
            raise RuntimeError("storage reserve would be exceeded; import remains pending")

    def put(self, stream, *, max_bytes: int):
        self.check_space()
        tmpdir = self.root / "tmp"
        tmpdir.mkdir(exist_ok=True)
        fd, name = tempfile.mkstemp(dir=tmpdir)
        sha = hashlib.sha256()
        size = 0
        try:
            with os.fdopen(fd, "wb") as out:
                while chunk := stream.read(1024 * 1024):
                    size += len(chunk)
                    if size > max_bytes:
                        raise ValueError("file exceeds configured size limit")
                    self.check_space(len(chunk))
                    sha.update(chunk)
                    out.write(chunk)
                out.flush()
                os.fsync(out.fileno())
            hexdigest = sha.hexdigest()
            key = f"originals/{hexdigest[:2]}/{hexdigest}"
            # put must not try to download an original that has not been uploaded yet.
            target = self.root / key
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                os.unlink(name)
            else:
                os.replace(name, target)
            if self.mirror:
                client, bucket = self.mirror
                client.upload_file(str(target), bucket, "research/" + key)
            return key, hexdigest, size
        finally:
            if os.path.exists(name):
                os.unlink(name)
