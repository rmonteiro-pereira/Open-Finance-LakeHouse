"""Register supplied inputs and publish complete quote-date revisions."""

from pathlib import Path

from psycopg.types.json import Jsonb

from . import cotahist
from .db import Database, digest, uid
from .storage import Store

SOURCES = {"b3_cotahist", "b3_bulletin", "mub3"}


def reprocess(db, import_id, *, actor="cli", supersedes=None):
    with db.connect() as con:
        row = con.execute("SELECT * FROM imports WHERE id=%s FOR UPDATE", (import_id,)).fetchone()
        if not row:
            raise LookupError("import not found")
        active = con.execute(
            "SELECT id FROM jobs WHERE payload->>'import_id'=%s AND state IN ('queued','running')",
            (import_id,),
        ).fetchone()
        if active:
            return {"job_id": active["id"]}
        metadata = row["metadata"]
        if supersedes:
            previous = con.execute("SELECT * FROM imports WHERE id=%s", (supersedes,)).fetchone()
            if (
                not previous
                or previous["source"] != row["source"]
                or previous["state"] != "published"
            ):
                raise ValueError("supersedes must identify a published input from the same source")
            metadata = {**metadata, "supersedes": supersedes}
        con.execute(
            "UPDATE imports SET state='queued',metadata=%s,error=NULL WHERE id=%s",
            (Jsonb(metadata), import_id),
        )
        job = db.enqueue_on(con, "import", {"import_id": import_id}, actor)
        con.execute(
            "INSERT INTO audit(actor,action,target,detail) VALUES (%s,'reprocess',%s,%s)",
            (actor, import_id, Jsonb({"supersedes": supersedes})),
        )
        return {"job_id": job}


def register(
    db: Database, store: Store, path: Path, source: str, metadata=None, actor="cli", name=None
):
    if source not in SOURCES:
        raise ValueError("unknown research source")
    with path.open("rb") as stream:
        key, sha, size = store.put(stream, max_bytes=db.settings.max_upload_bytes)
    version = cotahist.VERSION if source == "b3_cotahist" else "archive-v1"
    identity = digest([source, sha, version])
    with db.connect() as con:
        con.execute(
            """INSERT INTO imports(id,source,sha256,name,size,object_key,parser_version,metadata)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (identity, source, sha, name or path.name, size, key, version, Jsonb(metadata or {})),
        )
        row = con.execute("SELECT * FROM imports WHERE id=%s FOR UPDATE", (identity,)).fetchone()
        if row["state"] == "registered":
            job = db.enqueue_on(con, "import", {"import_id": identity}, actor)
            con.execute("UPDATE imports SET state='queued' WHERE id=%s", (identity,))
        else:
            job_row = con.execute(
                "SELECT id FROM jobs WHERE payload->>'import_id'=%s ORDER BY created_at DESC LIMIT 1",
                (identity,),
            ).fetchone()
            job = job_row["id"] if job_row else None
    return {"import_id": identity, "job_id": job, "sha256": sha, "bytes": size}


def import_quotes(db: Database, store: Store, import_id: str, heartbeat=lambda _: None):
    with db.connect() as guard:
        guard.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (import_id,))
        row = guard.execute("SELECT * FROM imports WHERE id=%s", (import_id,)).fetchone()
        if row is None:
            raise ValueError("unknown import")
        if row["state"] == "published":
            return row["counts"]
        output_key = f"staging/{import_id}"
        output = store.path(output_key)
        # A retry discards only its unpublished staging; the original is immutable.
        import shutil

        if output.exists():
            shutil.rmtree(output)
        result = cotahist.stage(
            store.path(row["object_key"]),
            output,
            row["sha256"],
            input_name=row["name"],
            batch_rows=db.settings.batch_rows,
            max_uncompressed_bytes=db.settings.max_uncompressed_bytes,
            heartbeat=heartbeat,
        )
        heartbeat(result["counts"])
        # One writer for the new lane. It is intentionally outside the old daily barrier.
        guard.execute("SELECT pg_advisory_xact_lock(hashtext('ofl-research-quotes'))")
        from datetime import datetime

        period = result["period"]
        if result["frequency"] == 1:
            period_start, period_end = f"{period}-01-01", f"{period}-12-31"
        elif result["frequency"] == 2:
            import calendar

            month, year = int(period[:2]), int(period[2:])
            period_start = f"{year}-{month:02d}-01"
            period_end = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"
        else:
            period_start = period_end = datetime.strptime(period, "%d%m%Y").date().isoformat()
        # Complete-period corrections can remove an entire previously present day.
        scope_dates = sorted(
            set(result["dates"])
            | {
                r["date"]
                for r in guard.execute(
                    "SELECT date::text AS date FROM quote_dates WHERE date BETWEEN %s AND %s",
                    (period_start, period_end),
                ).fetchall()
            }
        )
        guard.execute(
            """INSERT INTO quote_files(import_id,frequency,generated,parquet_key,dates,supersedes)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(import_id) DO UPDATE SET
            parquet_key=excluded.parquet_key,dates=excluded.dates""",
            (
                import_id,
                result["frequency"],
                result["generated"],
                output_key,
                Jsonb(result["dates"]),
                row["metadata"].get("supersedes"),
            ),
        )
        changed = []
        for day in scope_dates:
            current = guard.execute(
                """SELECT f.* FROM quote_dates d JOIN quote_files f
                ON f.import_id=d.import_id WHERE d.date=%s""",
                (day,),
            ).fetchone()
            incoming = (result["frequency"], result["generated"])
            if current:
                priority = (current["frequency"], current["generated"])
                if incoming < priority:
                    continue
                if incoming == priority and current["import_id"] != import_id:
                    if row["metadata"].get("supersedes") != current["import_id"]:
                        raise ValueError(
                            f"{day}: conflicting equal-priority input; explicit supersedes required"
                        )
            guard.execute(
                """INSERT INTO quote_dates(date,import_id) VALUES (%s,%s)
                ON CONFLICT(date) DO UPDATE SET import_id=excluded.import_id""",
                (day, import_id),
            )
            changed.append(day)
        if changed:
            _publish_quotes(db, store, guard, import_id, changed, heartbeat)
        guard.execute(
            "UPDATE imports SET state='published',counts=%s,error=NULL,updated_at=now() WHERE id=%s",
            (Jsonb(result["counts"]), import_id),
        )
        return result["counts"]


def _publish_quotes(db, store, con, import_id, dates, heartbeat):
    import duckdb
    import pyarrow as pa
    import pyarrow.compute as pc
    from deltalake import DeltaTable, write_deltalake

    query = duckdb.connect()
    query.execute("SET memory_limit='512MB'")
    spill = store.path("tmp/duckdb")
    spill.mkdir(parents=True, exist_ok=True)
    query.execute("SET temp_directory=?", (str(spill),))
    delta_uri = str(store.path("silver/fact_b3_quote"))
    published = con.execute("""SELECT r.* FROM pointers p JOIN releases r ON r.id=p.release_id
        WHERE p.kind='quotes'""").fetchone()
    if DeltaTable.is_deltatable(delta_uri) and published:
        delta = DeltaTable(delta_uri)
        # A process can die after its Delta commit but before its PostgreSQL commit.
        # Restore the published snapshot before applying the next transaction.
        if delta.version() != published["delta_version"]:
            delta.restore(published["delta_version"])
    elif DeltaTable.is_deltatable(delta_uri):
        # An interrupted first publication has no committed snapshot to restore.
        # Retain its files for diagnosis, but do not include them in a new release.
        orphan = store.path(f"orphaned/{uid()}")
        orphan.parent.mkdir(exist_ok=True)
        Path(delta_uri).rename(orphan)
    try:
        step = db.settings.publication_days
        for start in range(0, len(dates), step):
            batch = dates[start : start + step]
            authority = con.execute(
                """SELECT d.date::text AS date,f.parquet_key,i.sha256 FROM quote_dates d
                JOIN quote_files f ON f.import_id=d.import_id JOIN imports i ON i.id=f.import_id
                WHERE d.date=ANY(%s::date[])""",
                (batch,),
            ).fetchall()
            paths = sorted({str(store.path(a["parquet_key"]) / "*.parquet") for a in authority})
            query.register("authority", pa.Table.from_pylist(authority))
            query.read_parquet(paths).create_view("input_rows", replace=True)
            conflict = query.execute("""SELECT record_id FROM input_rows q JOIN authority a
                ON q.date=CAST(a.date AS DATE) AND q.source_hash=a.sha256
                GROUP BY record_id HAVING count(DISTINCT row_hash)>1 LIMIT 1""").fetchone()
            if conflict:
                raise ValueError(f"conflicting quote identity {conflict[0]}")
            table = query.execute("""SELECT q.* EXCLUDE(rn) FROM (
                SELECT q.*,row_number() OVER(PARTITION BY record_id ORDER BY source_line) rn
                FROM input_rows q JOIN authority a ON q.date=CAST(a.date AS DATE)
                AND q.source_hash=a.sha256) q WHERE rn=1""").to_arrow_table()
            table = table.append_column("year", pc.year(table["date"]).cast(pa.int32()))
            predicate = "date IN (" + ",".join(f"DATE '{d}'" for d in batch) + ")"
            write_deltalake(
                delta_uri,
                table,
                mode="overwrite",
                partition_by=["year"],
                predicate=predicate if DeltaTable.is_deltatable(delta_uri) else None,
            )
            heartbeat({"published_dates": min(start + step, len(dates)), "total_dates": len(dates)})
        release = uid()
        version = DeltaTable(delta_uri).version()
        con.execute(
            """INSERT INTO releases(id,kind,delta_uri,delta_version,metadata)
            VALUES (%s,'quotes',%s,%s,%s)""",
            (release, delta_uri, version, Jsonb({"import_id": import_id})),
        )
        con.execute(
            """INSERT INTO pointers(kind,release_id) VALUES ('quotes',%s)
            ON CONFLICT(kind) DO UPDATE SET release_id=excluded.release_id""",
            (release,),
        )
    finally:
        query.close()


def coverage(db):
    expected = list(range(1986, __import__("datetime").date.today().year + 1))
    years = db.rows(
        "SELECT extract(year FROM date)::integer AS year,count(*) AS trading_dates FROM quote_dates GROUP BY year ORDER BY year"
    )
    return {
        "imports": db.rows(
            "SELECT source,state,count(*) AS files,sum(size) AS bytes FROM imports GROUP BY source,state"
        ),
        "quote_years": years,
        "items": db.rows(
            "SELECT source,item_type,count(*) AS items FROM items GROUP BY source,item_type"
        ),
        "assets": db.rows("SELECT state,count(*) AS assets FROM assets GROUP BY state"),
        "jobs": db.rows("SELECT state,count(*) AS jobs FROM jobs GROUP BY state"),
        "reviews": db.rows("SELECT state,count(*) AS tables FROM candidates GROUP BY state"),
        "expected_years": expected,
        "missing_years": sorted(set(expected) - {r["year"] for r in years}),
        "search": db.rows(
            "SELECT method,count(*) AS passages FROM passages WHERE active GROUP BY method"
        ),
        "embedding_models": db.rows(
            "SELECT e.model,e.revision,count(*) AS passages FROM embeddings e JOIN passages p ON p.id=e.passage_id WHERE p.active GROUP BY e.model,e.revision"
        ),
    }
