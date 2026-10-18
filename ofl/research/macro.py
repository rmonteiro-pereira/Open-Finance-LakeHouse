"""Pinned, checksum-verified core snapshots for private data work. No arbitrary SQL."""

import csv
import hashlib
import io
import json
import re
from datetime import datetime

from psycopg.types.json import Jsonb

from .db import digest
from .library import LibrarySource

IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
SERIES = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,127}$")
SHA = re.compile(r"^[a-f0-9]{64}$")
QUALIFICATION = (
    "Published macro snapshot; values preserve the source numeric representation. "
    "Observation dates are not publication dates. This snapshot does not establish historical information availability."
)


def _path(key, prefix):
    if (
        not isinstance(key, str)
        or not key.startswith(prefix)
        or "\\" in key
        or any(part in {"", ".", ".."} for part in key.split("/"))
    ):
        raise ValueError("snapshot object is outside its immutable release prefix")
    return key


def sync_macro(db, store, *, source=None):
    source = source or LibrarySource(db.settings)
    latest = json.loads(
        source.read("latest.json", db.settings.bridge_metadata_bytes, snapshot=True)
    )
    manifest_key = _path(latest["manifest"], "snapshots/")
    raw_manifest = source.read(manifest_key, db.settings.bridge_metadata_bytes, snapshot=True)
    manifest = json.loads(raw_manifest)
    snapshot = str(manifest.get("run_id", ""))
    if not IDENTIFIER.fullmatch(snapshot):
        raise ValueError("invalid snapshot identifier")
    prefix = f"snapshots/{snapshot}/"
    if manifest_key != prefix + "manifest.json":
        raise ValueError("snapshot identity does not match manifest key")
    generated = datetime.fromisoformat(manifest["generated_at"].replace("Z", "+00:00"))
    if generated.tzinfo is None:
        raise ValueError("snapshot generation timestamp needs timezone")
    catalog_key = _path(manifest["catalog"], prefix)
    catalog = json.loads(source.read(catalog_key, db.settings.bridge_metadata_bytes, snapshot=True))
    records = catalog.get("series")
    if not isinstance(records, list) or not 1 <= len(records) <= 10000:
        raise ValueError("snapshot series catalog is invalid")
    seen = set()
    for row in records:
        if (
            not isinstance(row, dict)
            or not SERIES.fullmatch(str(row.get("key", "")))
            or row["key"] in seen
        ):
            raise ValueError("snapshot series identity is invalid or duplicated")
        seen.add(row["key"])
    allowed = {row["key"] for row in records if row.get("redistribution") == "open"}
    descriptor = manifest.get("observations") or {}
    key = _path(descriptor["path"], prefix)
    expected_sha = str(descriptor.get("sha256", ""))
    if not SHA.fullmatch(expected_sha):
        raise ValueError("snapshot observations require a SHA-256 checksum")
    fingerprint = digest([manifest, catalog])
    existing = db.rows(
        "SELECT fingerprint,observations_key FROM macro_snapshots WHERE id=%s", (snapshot,)
    )
    if existing:
        if existing[0]["fingerprint"] != fingerprint:
            raise ValueError("an existing immutable snapshot was changed")
        # A cache miss may restore the original through the existing private mirror.
        if not store.path(existing[0]["observations_key"]).is_file():
            raise ValueError(
                "cached snapshot is missing; restore the private original before serving"
            )
    else:
        payload = source.read(key, db.settings.bridge_asset_bytes, snapshot=True)
        if hashlib.sha256(payload).hexdigest() != expected_sha:
            raise ValueError("snapshot observations checksum mismatch")
        original_key, _, _ = store.put(
            io.BytesIO(payload), max_bytes=db.settings.bridge_asset_bytes
        )
        statistics = _validate_observations(store.path(original_key), allowed)
        catalog = {
            **catalog,
            "series": [
                {
                    **row,
                    "source_declared_rows": row.get("rows"),
                    "rows": statistics.get(row["key"], {}).get("rows", 0),
                    "first_date": statistics.get(row["key"], {}).get("first_date"),
                    "last_date": statistics.get(row["key"], {}).get("last_date"),
                    "coverage_validated": True,
                }
                for row in records
            ],
        }
        with db.connect() as con:
            con.execute(
                """INSERT INTO macro_snapshots(id,generated_at,fingerprint,observations_key,sha256,manifest,catalog)
                VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (
                    snapshot,
                    generated,
                    fingerprint,
                    original_key,
                    expected_sha,
                    Jsonb(manifest),
                    Jsonb(catalog),
                ),
            )
            stored = con.execute(
                "SELECT fingerprint FROM macro_snapshots WHERE id=%s", (snapshot,)
            ).fetchone()
            if stored["fingerprint"] != fingerprint:
                raise ValueError("concurrent immutable snapshot conflict")
    with db.connect() as con:
        con.execute(
            """INSERT INTO macro_pointer(singleton,snapshot_id) VALUES (true,%s)
            ON CONFLICT(singleton) DO UPDATE SET snapshot_id=excluded.snapshot_id""",
            (snapshot,),
        )
    return {"snapshot_id": snapshot, "open_series": len(allowed), "qualification": QUALIFICATION}


def _validate_observations(path, allowed):
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pq.read_schema(path)
    if set(schema.names) != {"series_id", "date", "value"} or not pa.types.is_date(
        schema.field("date").type
    ):
        raise ValueError("snapshot observations schema is incompatible")
    value_type = schema.field("value").type
    if not (
        pa.types.is_floating(value_type)
        or pa.types.is_integer(value_type)
        or pa.types.is_decimal(value_type)
    ):
        raise ValueError("snapshot values must be numeric")
    with duckdb.connect() as con:
        con.execute("SET memory_limit='512MB'")
        con.from_parquet(str(path)).create_view("observations")
        invalid = con.execute(
            "SELECT count(*) FROM observations WHERE series_id IS NULL OR date IS NULL OR (value IS NOT NULL AND NOT isfinite(value))"
        ).fetchone()[0]
        duplicates = con.execute(
            "SELECT count(*) FROM (SELECT series_id,date FROM observations GROUP BY ALL HAVING count(*)>1)"
        ).fetchone()[0]
        actual = {
            row[0] for row in con.execute("SELECT DISTINCT series_id FROM observations").fetchall()
        }
        if invalid or duplicates or not actual <= allowed:
            raise ValueError("snapshot has invalid, duplicate, or non-open observations")
        return {
            key: {"rows": count, "first_date": str(first), "last_date": str(last)}
            for key, count, first, last in con.execute(
                "SELECT series_id,count(*),min(date),max(date) FROM observations GROUP BY series_id"
            ).fetchall()
        }


def snapshot_record(db, snapshot_id=None):
    if snapshot_id:
        rows = db.rows("SELECT * FROM macro_snapshots WHERE id=%s", (snapshot_id,))
    else:
        rows = db.rows(
            "SELECT s.* FROM macro_pointer p JOIN macro_snapshots s ON s.id=p.snapshot_id"
        )
    if not rows:
        raise LookupError("macro snapshot is not available")
    return rows[0]


def catalog(db, *, snapshot_id=None, query=None):
    current = snapshot_record(db, snapshot_id)
    records = [row for row in current["catalog"]["series"] if row.get("redistribution") == "open"]
    if query:
        needle = query.casefold()
        records = [
            row
            for row in records
            if needle
            in " ".join(str(row.get(k, "")) for k in ("key", "name", "domain", "source")).casefold()
        ]
    return {
        "snapshot_id": current["id"],
        "generated_at": current["generated_at"],
        "series": records,
        "qualification": QUALIFICATION,
    }


def series_query(
    db, store, series_id, *, snapshot_id=None, date_from=None, date_to=None, limit=1000, offset=0
):
    import duckdb

    if not SERIES.fullmatch(series_id) or not 1 <= limit <= 100000 or offset < 0:
        raise ValueError("invalid series query bounds")
    if date_from and date_to and str(date_from) > str(date_to):
        raise ValueError("date interval reversed")
    current = snapshot_record(db, snapshot_id)
    metadata = next(
        (
            row
            for row in current["catalog"]["series"]
            if row["key"] == series_id and row.get("redistribution") == "open"
        ),
        None,
    )
    if metadata is None:
        raise LookupError("series is unavailable in this snapshot")
    path = store.path(current["observations_key"])
    with path.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != current["sha256"]:
            raise ValueError("cached snapshot checksum mismatch")
    clauses, params = ["series_id=?"], [series_id]
    if date_from:
        clauses.append("date>=?")
        params.append(date_from)
    if date_to:
        clauses.append("date<=?")
        params.append(date_to)
    where = " AND ".join(clauses)
    with duckdb.connect() as con:
        con.execute("SET memory_limit='512MB'")
        con.from_parquet(str(path)).create_view("observations")
        count, first, last, missing = con.execute(
            f"SELECT count(*),min(date),max(date),count(*) FILTER(WHERE value IS NULL) FROM observations WHERE {where}",
            params,
        ).fetchone()
        values = con.execute(
            f"SELECT date,value FROM observations WHERE {where} ORDER BY date LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()
    return {
        "snapshot_id": current["id"],
        "snapshot_generated_at": current["generated_at"],
        "source_dataset_sha256": current["sha256"],
        "scope": {"date_from": date_from, "date_to": date_to},
        "series": metadata,
        "rows": [
            {"date": day, "value": None if value is None else str(value)} for day, value in values
        ],
        "matching_rows": count,
        "returned_rows": len(values),
        "offset": offset,
        "has_more": offset + len(values) < count,
        "coverage": {"first_observation": first, "last_observation": last, "null_values": missing},
        "qualification": QUALIFICATION,
    }


def export_series(db, store, series_id, *, snapshot_id, date_from=None, date_to=None):
    result = series_query(
        db,
        store,
        series_id,
        snapshot_id=snapshot_id,
        date_from=date_from,
        date_to=date_to,
        limit=100000,
    )
    if result["has_more"]:
        raise ValueError("export exceeds 100,000 rows; narrow the interval")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["series_id", "observation_date", "value", "unit", "frequency", "snapshot_id"])
    unit = str(result["series"].get("unit", ""))
    frequency = str(result["series"].get("frequency", ""))

    # CSV strings that could be interpreted as spreadsheet formulas are escaped.
    def text(value):
        return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value

    for row in result["rows"]:
        writer.writerow(
            [series_id, row["date"], row["value"], text(unit), text(frequency), snapshot_id]
        )
    return output.getvalue(), result
