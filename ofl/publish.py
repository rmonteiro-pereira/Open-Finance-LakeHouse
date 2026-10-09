"""Publish a public, versioned snapshot of the lakehouse.

The lakehouse sits on a private cluster that accepts no inbound traffic. Instead of
opening it, the pipeline pushes outward: after each gold run this module copies what
may be shown in public to an object store that a website (or anyone) reads over HTTPS.
The site is then one consumer of a data product, and keeps serving the last snapshot
when the cluster is down or busy.

Layout written to the target::

    snapshots/<run_id>/manifest.json            what this snapshot holds, with checksums
    snapshots/<run_id>/catalog.json             the series catalogue, from the registry
    snapshots/<run_id>/marts/<mart>.parquet     one file per published gold mart
    snapshots/<run_id>/series/observations.parquet   open single-value series, long format
    index.json                                  the snapshots kept, newest first
    latest.json                                 pointer to the newest snapshot (written last)

Parquet rather than Delta: a browser can query Parquet over HTTP range requests
(DuckDB-WASM), and a snapshot is immutable anyway.

**Licence tiers.** Every series carries ``redistribution`` in the registry (``open``,
``derived`` or ``private``). A mart takes the most restrictive tier among the series
that feed it (``MART_INPUTS``): ``private`` marts are never written, ``derived`` marts
are written for charts but flagged ``download: false``, ``open`` marts are offered for
download. Raw observations are exported for ``open`` series only.

The target is a bucket (``OFL_PUBLISH_BUCKET``, optional ``OFL_PUBLISH_PREFIX``). By default
it lives on the lakehouse's own object store, served read-only by the deployment; set
``OFL_PUBLISH_ENDPOINT``, ``OFL_PUBLISH_ACCESS_KEY`` and ``OFL_PUBLISH_SECRET_KEY`` to push
to another S3-compatible store instead. ``OFL_PUBLISH_DIR`` selects a local folder (tests
and dry runs).
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from ofl.platform.io import gold_uri, silver_uri
from ofl.platform.logging import get_logger
from ofl.registry import REDISTRIBUTION_TIERS, Registry, load_registry
from ofl.transform.gold.runner import MODELS

if TYPE_CHECKING:
    import duckdb

log = get_logger(__name__)

SCHEMA_VERSION = 1

# The series each mart is computed from. It decides the mart's licence tier, so a mart
# missing here is not published at all (and a test fails until it is declared).
MART_INPUTS: dict[str, tuple[str, ...]] = {
    "mart_real_interest": ("ipca", "selic_meta"),
    "mart_inflation_panel": ("ipca", "ipca_15", "inpc", "igp_m", "igp_di", "igp_10"),
    "mart_fx": ("usd_brl", "eur_brl"),
    "mart_macro_dashboard": ("selic_meta", "ipca", "usd_brl", "divida_pib"),
    # Reads all of fact_treasury, which also receives the ANBIMA sandbox rows. The mart
    # has no `source` column to filter on, so it stays private until it does.
    "mart_yield_curve": ("tesouro_direto", "anbima"),
    "mart_equity_daily": (
        "yahoo_etf",
        "yahoo_commodity",
        "yahoo_currency",
        "yahoo_global",
        "b3",
        "b3_cotahist",
    ),
    "mart_futures_curve": ("b3_deriv_quotes", "b3_oi", "b3_instruments"),
    "mart_open_interest": ("b3_oi",),
    "mart_di_curve_points": ("b3_deriv_quotes", "b3_oi", "b3_instruments"),
    "mart_di_curve_slope": ("b3_deriv_quotes", "b3_oi", "b3_instruments"),
}

# The column that says how recent a mart is, in order of preference.
_DATE_COLUMNS = ("date", "trade_date", "month")

_IMMUTABLE = "public, max-age=31536000, immutable"
_POINTER = "public, max-age=60"


class PublishError(RuntimeError):
    """Nothing publishable was produced; the previous snapshot stays the latest."""


class Target(Protocol):
    def read(self, key: str) -> bytes | None: ...
    def write(self, key: str, data: bytes, content_type: str, cache_control: str) -> None: ...
    def list(self, prefix: str) -> list[str]: ...
    def delete(self, keys: list[str]) -> None: ...


class LocalTarget:
    def __init__(self, root: Path | str):
        self.root = Path(root)

    def read(self, key: str) -> bytes | None:
        path = self.root / key
        return path.read_bytes() if path.exists() else None

    def write(self, key: str, data: bytes, content_type: str, cache_control: str) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def list(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file())

    def delete(self, keys: list[str]) -> None:
        for key in keys:
            (self.root / key).unlink(missing_ok=True)


class S3Target:
    """A bucket on any S3-compatible store."""

    def __init__(
        self, bucket: str, *, endpoint: str, access_key: str, secret_key: str, prefix: str = ""
    ):
        import boto3
        from botocore.exceptions import ClientError

        self._missing = ClientError
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=os.getenv("OFL_PUBLISH_REGION", "auto"),
        )

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def read(self, key: str) -> bytes | None:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=self._key(key))["Body"].read()
        except self._missing as exc:
            if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
                return None
            raise

    def write(self, key: str, data: bytes, content_type: str, cache_control: str) -> None:
        self.s3.put_object(
            Bucket=self.bucket,
            Key=self._key(key),
            Body=data,
            ContentType=content_type,
            CacheControl=cache_control,
        )

    def list(self, prefix: str) -> list[str]:
        strip = len(self.prefix) + 1 if self.prefix else 0
        keys: list[str] = []
        pages = self.s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=self._key(prefix)
        )
        for page in pages:
            keys += [obj["Key"][strip:] for obj in page.get("Contents", [])]
        return keys

    def delete(self, keys: list[str]) -> None:
        for start in range(0, len(keys), 1000):
            batch = [{"Key": self._key(k)} for k in keys[start : start + 1000]]
            self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})


def default_target() -> Target:
    """``OFL_PUBLISH_DIR`` selects a local folder; otherwise the ``OFL_PUBLISH_BUCKET`` bucket."""
    local = os.getenv("OFL_PUBLISH_DIR")
    if local:
        return LocalTarget(local)
    bucket = os.getenv("OFL_PUBLISH_BUCKET")
    if not bucket:
        raise PublishError("no publish target: set OFL_PUBLISH_DIR or OFL_PUBLISH_BUCKET")
    # With only the bucket named, the target is a bucket on the lakehouse's own store.
    from ofl.config import get_settings

    lake = get_settings()
    return S3Target(
        bucket,
        endpoint=os.getenv("OFL_PUBLISH_ENDPOINT") or lake.minio_endpoint,
        access_key=os.getenv("OFL_PUBLISH_ACCESS_KEY") or lake.minio_user,
        secret_key=os.getenv("OFL_PUBLISH_SECRET_KEY") or lake.minio_password,
        prefix=os.getenv("OFL_PUBLISH_PREFIX", ""),
    )


def mart_tier(mart: str, registry: Registry) -> str:
    """Most restrictive tier among the mart's inputs; undeclared or unknown is private."""
    inputs = MART_INPUTS.get(mart)
    if not inputs or any(key not in registry.series for key in inputs):
        return "private"
    tiers = [registry.series[key].redistribution or "private" for key in inputs]
    return max(tiers, key=REDISTRIBUTION_TIERS.index)


def _lake_relation(layer: str, table: str) -> str:
    uri = gold_uri(table) if layer == "gold" else silver_uri(table)
    return f"delta_scan('{uri}')"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_parquet(con: duckdb.DuckDBPyConnection, select: str, path: Path) -> dict:
    """Run ``select`` into a Parquet file and describe what landed."""
    con.execute(f"COPY ({select}) TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    scan = f"read_parquet('{path.as_posix()}')"
    columns = [
        {"name": row[0], "type": row[1]}
        for row in con.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()
    ]
    info = {
        "rows": con.execute(f"SELECT count(*) FROM {scan}").fetchone()[0],
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "columns": columns,
    }
    names = {c["name"] for c in columns}
    date_column = next((c for c in _DATE_COLUMNS if c in names), None)
    if date_column and info["rows"]:
        low, high = con.execute(
            f"SELECT min({date_column})::VARCHAR, max({date_column})::VARCHAR FROM {scan}"
        ).fetchone()
        info |= {"date_column": date_column, "min_date": low, "max_date": high}
    return info


def _catalog(registry: Registry, stats: dict[str, tuple[int, str, str]]) -> dict:
    series = []
    for s in registry.active():
        rows, first, last = stats.get(s.key, (None, None, None))
        series.append(
            {
                "key": s.key,
                "name": s.name,
                "domain": s.domain,
                "source": s.handler,
                "category": s.category,
                "unit": s.unit,
                "frequency": s.frequency,
                "fact": s.fact,
                "redistribution": s.redistribution,
                "rows": rows,
                "first_date": first,
                "last_date": last,
            }
        )
    return {"schema_version": SCHEMA_VERSION, "series": series}


def _json(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def publish(
    con: duckdb.DuckDBPyConnection,
    target: Target,
    *,
    run_id: str | None = None,
    registry: Registry | None = None,
    relation: Callable[[str, str], str] = _lake_relation,
    keep: int = 30,
) -> dict:
    """Build one snapshot and make it the latest. Returns its manifest.

    ``relation(layer, table)`` gives the SQL relation to read a lakehouse table from;
    the default scans the Delta tables, tests pass in-memory ones. Everything is built
    locally first, so a run that produces no mart raises :class:`PublishError` before
    anything is uploaded and ``latest.json`` keeps pointing at the previous snapshot.
    """
    registry = registry or load_registry()
    now = datetime.now(UTC)
    run_id = run_id or now.strftime("%Y%m%dT%H%M%SZ")
    base = f"snapshots/{run_id}"
    marts: list[dict] = []
    skipped: list[dict] = []
    uploads: list[tuple[str, Path]] = []

    with tempfile.TemporaryDirectory(prefix="ofl-publish-") as tmp:
        work = Path(tmp)
        for name in MODELS:
            tier = mart_tier(name, registry)
            if tier == "private":
                skipped.append({"name": name, "reason": "private"})
                continue
            path = work / f"{name}.parquet"
            try:
                info = _write_parquet(con, f"SELECT * FROM {relation('gold', name)}", path)
            except Exception as exc:  # noqa: BLE001 - a mart that is not there is reported, not fatal
                log.warning("publish_mart_unavailable", mart=name, error=str(exc))
                skipped.append({"name": name, "reason": "missing"})
                continue
            if not info["rows"]:
                skipped.append({"name": name, "reason": "empty"})
                continue
            key = f"{base}/marts/{name}.parquet"
            marts.append(
                {"name": name, "path": key, "redistribution": tier, "download": tier == "open"}
                | info
                | {"inputs": list(MART_INPUTS[name])}
            )
            uploads.append((key, path))
            log.info("publish_mart", mart=name, rows=info["rows"], tier=tier)

        if not marts:
            raise PublishError(
                "no gold mart could be published; the previous snapshot stays latest"
            )

        open_keys = sorted(s.key for s in registry.active() if s.redistribution == "open")
        observations = None
        stats: dict[str, tuple[int, str, str]] = {}
        if open_keys:
            quoted = ", ".join(f"'{k}'" for k in open_keys)
            path = work / "observations.parquet"
            try:
                info = _write_parquet(
                    con,
                    f"SELECT series_id, date, value FROM {relation('silver', 'fact_observation')} "
                    f"WHERE series_id IN ({quoted}) ORDER BY series_id, date",
                    path,
                )
                rows = con.execute(
                    f"SELECT series_id, count(*), min(date)::VARCHAR, max(date)::VARCHAR "
                    f"FROM read_parquet('{path.as_posix()}') GROUP BY 1"
                ).fetchall()
                stats = {r[0]: (r[1], r[2], r[3]) for r in rows}
                key = f"{base}/series/observations.parquet"
                observations = {"path": key, "series": len(stats)} | info
                uploads.append((key, path))
            except Exception as exc:  # noqa: BLE001 - the marts are still worth publishing
                log.warning("publish_observations_unavailable", error=str(exc))

        manifest = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "generated_at": now.isoformat(timespec="seconds"),
            "marts": marts,
            "observations": observations,
            "catalog": f"{base}/catalog.json",
            "skipped": skipped,
        }
        for key, path in uploads:
            target.write(key, path.read_bytes(), "application/vnd.apache.parquet", _IMMUTABLE)

    target.write(
        f"{base}/catalog.json", _json(_catalog(registry, stats)), "application/json", _IMMUTABLE
    )
    target.write(f"{base}/manifest.json", _json(manifest), "application/json", _IMMUTABLE)
    _update_pointers(target, manifest, keep=keep)
    log.info("publish_done", run_id=run_id, marts=len(marts), skipped=len(skipped))
    return manifest


def _update_pointers(target: Target, manifest: dict, *, keep: int) -> None:
    """Add the snapshot to ``index.json``, drop the oldest beyond ``keep``, move ``latest.json``."""
    run_id = manifest["run_id"]
    raw = target.read("index.json")
    known = json.loads(raw)["snapshots"] if raw else []
    entry = {
        "run_id": run_id,
        "generated_at": manifest["generated_at"],
        "manifest": f"snapshots/{run_id}/manifest.json",
        "marts": {
            m["name"]: {"rows": m["rows"], "max_date": m.get("max_date")} for m in manifest["marts"]
        },
    }
    snapshots = [entry] + [s for s in known if s["run_id"] != run_id]
    snapshots.sort(key=lambda s: s["generated_at"], reverse=True)
    kept, dropped = snapshots[:keep], snapshots[keep:]
    target.write(
        "index.json",
        _json({"schema_version": SCHEMA_VERSION, "snapshots": kept}),
        "application/json",
        _POINTER,
    )
    target.write(
        "latest.json",
        _json({k: entry[k] for k in ("run_id", "generated_at", "manifest")}),
        "application/json",
        _POINTER,
    )
    for old in dropped:
        target.delete(target.list(f"snapshots/{old['run_id']}/"))
        log.info("publish_pruned", run_id=old["run_id"])


def run_publish(*, run_id: str | None = None, keep: int = 30) -> dict:
    """Publish from the live lakehouse (reads only) to the configured target."""
    import duckdb

    from ofl.transform.gold.runner import configure_minio

    target = default_target()
    con = duckdb.connect()
    try:
        configure_minio(con)
        return publish(con, target, run_id=run_id, keep=keep)
    finally:
        con.close()
