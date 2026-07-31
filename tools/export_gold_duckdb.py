"""Export the gold marts from MinIO into a portable local DuckDB file (+ parquet siblings).

READ-ONLY GUARANTEE
-------------------
This script **only reads** from the ``lakehouse`` bucket. Every object-store access
goes through DuckDB's ``delta_scan`` (a reader), and the only writes are to the local
output directory. It never issues ``PUT``/``DELETE``, never touches bucket policies,
lifecycle or admin APIs, and never runs the gold transformation — it copies what the
pipeline already materialized at ``s3://<bucket>/gold/mart_*``.

Why this exists
---------------
Downstream consumers (notably the text-to-SQL agent) need the gold marts as a local,
portable artifact so they can answer questions without reaching into the cluster.

Usage
-----
    # endpoint + credentials come from the environment / .env (see ofl.config)
    python tools/export_gold_duckdb.py
    python tools/export_gold_duckdb.py --out-dir D:/somewhere/_artifacts
    python tools/export_gold_duckdb.py --marts mart_fx mart_yield_curve
    python tools/export_gold_duckdb.py --strict     # non-zero exit if any mart is missing

The default output is ``<repo parent>/_artifacts/ofl_gold.duckdb`` — deliberately
outside the repository, since the artifact is never committed.

Re-runnable: each mart is written with ``CREATE OR REPLACE TABLE`` and its parquet
sibling is overwritten, so running it twice yields the same result as running it once.

Configuration is the repo's existing one (``ofl.config.Settings``): ``MINIO_ENDPOINT``,
``MINIO_USER``, ``MINIO_PASSWORD``, ``AWS_REGION``, ``LAKEHOUSE_BUCKET`` — read from the
environment or ``.env``. No endpoints or secrets are hardcoded here, and none are printed.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from ofl.config import get_settings
from ofl.platform.io import gold_uri
from ofl.platform.logging import get_logger
from ofl.transform.gold.runner import MODELS, configure_minio

if TYPE_CHECKING:
    import duckdb

log = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = REPO_ROOT.parent / "_artifacts"
DB_NAME = "ofl_gold.duckdb"

# Row counts + source URIs of what was actually exported, so a consumer can tell a
# genuinely empty mart from one that was never exported. Not fabricated data.
MANIFEST_TABLE = "_export_manifest"


def _export_mart(
    con: "duckdb.DuckDBPyConnection", name: str, out_dir: Path, *, parquet: bool
) -> tuple[str, int | None]:
    """Copy one gold mart into the local database. Returns ``(status, rows)``.

    Status is ``ok`` (rows > 0), ``empty`` (mart exists but has no rows) or
    ``missing`` (not materialized / unreadable). Nothing is invented for the
    latter two — they are reported as-is.
    """
    uri = gold_uri(name)
    try:
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM delta_scan('{uri}')")
    except Exception as exc:  # noqa: BLE001
        log.warning("mart_unavailable", mart=name, error=str(exc))
        con.execute(f"DROP TABLE IF EXISTS {name}")
        return "missing", None

    rows = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    if parquet:
        target = (out_dir / f"{name}.parquet").as_posix()
        con.execute(f"COPY {name} TO '{target}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1)")
    log.info("mart_exported", mart=name, rows=rows, parquet=parquet)
    return ("ok" if rows else "empty"), rows


def _write_manifest(
    con: "duckdb.DuckDBPyConnection", results: dict[str, tuple[str, int | None]]
) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {MANIFEST_TABLE} (
            mart VARCHAR, status VARCHAR, rows BIGINT, source_uri VARCHAR, exported_at TIMESTAMP
        )
        """
    )
    for name, (status, rows) in results.items():
        con.execute(
            f"INSERT INTO {MANIFEST_TABLE} VALUES (?, ?, ?, ?, now())",
            [name, status, rows, gold_uri(name)],
        )


def export(
    marts: list[str], out_dir: Path, *, parquet: bool = True
) -> dict[str, tuple[str, int | None]]:
    import duckdb

    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / DB_NAME
    con = duckdb.connect(str(db_path))
    try:
        configure_minio(con)  # same MinIO wiring the gold runner uses — read paths only
        results = {name: _export_mart(con, name, out_dir, parquet=parquet) for name in marts}
        _write_manifest(con, results)
    finally:
        con.close()
    log.info("export_done", db=str(db_path), marts=len(results))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="export_gold_duckdb",
        description="Export gold marts from MinIO (read-only) to a local DuckDB file + parquet.",
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help="artifact directory")
    parser.add_argument("--marts", nargs="+", default=MODELS, help="subset of marts to export")
    parser.add_argument("--no-parquet", action="store_true", help="skip the parquet siblings")
    parser.add_argument("--strict", action="store_true", help="exit non-zero if a mart is missing")
    args = parser.parse_args(argv)

    unknown = [m for m in args.marts if m not in MODELS]
    if unknown:
        parser.error(f"unknown mart(s): {', '.join(unknown)}. Known: {', '.join(MODELS)}")

    settings = get_settings()
    out_dir = args.out_dir.resolve()
    # Host only — never the credentials.
    print(f"source : s3://{settings.bucket}/gold  @ {settings.minio_endpoint} (read-only)")
    print(f"target : {out_dir / DB_NAME}")

    results = export(args.marts, out_dir, parquet=not args.no_parquet)

    width = max(len(m) for m in results)
    print(f"\n{'mart'.ljust(width)}  status   rows")
    for name, (status, rows) in results.items():
        print(f"{name.ljust(width)}  {status.ljust(7)}  {'-' if rows is None else rows}")

    ok = sum(1 for status, _ in results.values() if status == "ok")
    missing = [n for n, (status, _) in results.items() if status == "missing"]
    print(f"\n{ok}/{len(results)} marts exported with rows > 0")
    if missing:
        print(f"missing / unreadable: {', '.join(missing)}")

    if not ok:
        print("no mart produced any rows — nothing usable was exported", file=sys.stderr)
        return 1
    return 1 if (args.strict and missing) else 0


if __name__ == "__main__":
    raise SystemExit(main())
