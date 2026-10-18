"""One-shot/continuous leased worker; no imports or OCR in HTTP request handlers."""

import threading
import time

from .archive import import_archive
from .db import uid
from .imports import import_quotes
from .processing import process_asset


def build_observations(db, store):
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    from .db import uid

    schema = pa.schema(
        [
            ("id", pa.string()),
            ("candidate_id", pa.string()),
            ("revision", pa.int32()),
            ("date", pa.date32()),
            ("date_end", pa.date32()),
            ("date_precision", pa.string()),
            ("date_text", pa.string()),
            ("subject", pa.string()),
            ("market", pa.string()),
            ("metric", pa.string()),
            ("value", pa.decimal128(38, 12)),
            ("unit", pa.string()),
            ("currency", pa.string()),
            ("evidence", pa.string()),
        ]
    )
    import json

    with db.connect() as con:
        con.execute("SELECT pg_advisory_xact_lock(hashtext('ofl-research-observations'))")
        # Immutable release URI avoids rollback ambiguity across concurrent reviews.
        release = uid()
        uri = str(store.path(f"silver/historical/{release}"))
        count = 0
        with con.cursor(name="approved_observations") as cursor:
            cursor.execute("SELECT * FROM observations WHERE active ORDER BY id")

            def batches():
                nonlocal count
                while rows := cursor.fetchmany(db.settings.batch_rows):
                    store.check_space()
                    for row in rows:
                        row.pop("active")
                        row["evidence"] = json.dumps(row["evidence"])
                    count += len(rows)
                    yield from pa.Table.from_pylist(rows, schema=schema).to_batches()

            write_deltalake(uri, pa.RecordBatchReader.from_batches(schema, batches()))
        con.execute(
            "INSERT INTO releases(id,kind,delta_uri,delta_version) VALUES (%s,'observations',%s,%s)",
            (release, uri, DeltaTable(uri).version()),
        )
        con.execute(
            """INSERT INTO pointers(kind,release_id) VALUES ('observations',%s)
            ON CONFLICT(kind) DO UPDATE SET release_id=excluded.release_id""",
            (release,),
        )
    return {"release": release, "observations": count}


def execute(db, store, job, heartbeat):
    payload = job["payload"]
    if job["kind"] == "import":
        rows = db.rows("SELECT source FROM imports WHERE id=%s", (payload["import_id"],))
        if not rows:
            raise ValueError("unknown import")
        if rows[0]["source"] == "b3_cotahist":
            return import_quotes(db, store, payload["import_id"], heartbeat)
        return import_archive(db, store, payload["import_id"], heartbeat)
    if job["kind"] == "library_sync":
        if payload["kind"] == "macro":
            from .macro import sync_macro

            return sync_macro(db, store)
        from .library import sync_library

        return sync_library(
            db,
            store,
            payload["kind"],
            limit=payload.get("limit", 20),
            collection=payload.get("collection"),
            heartbeat=heartbeat,
        )
    if job["kind"] == "process_asset":
        return process_asset(db, store, payload["asset_id"], heartbeat)
    if job["kind"] == "build_observations":
        return build_observations(db, store)
    if job["kind"] == "embed":
        from .search import embed_pending

        return embed_pending(db, payload.get("limit", 1000), heartbeat)
    if job["kind"] == "export":
        from .query import export_quotes

        return export_quotes(db, store, job["id"], payload)
    if job["kind"] == "pipeline":
        from .orchestration import trigger

        return trigger(db.settings, payload["dag"], job["id"])
    raise ValueError("job kind not allowlisted")


def run_once(db, store):
    with db.connect() as guard:
        acquired = guard.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext('ofl-research-worker')) AS acquired"
        ).fetchone()["acquired"]
        if not acquired:
            return False
        return _run_claimed(db, store)


def _run_claimed(db, store):
    owner = uid()
    job = db.claim(owner)
    if not job:
        return False
    stop = threading.Event()
    lease_errors = []

    def heartbeat(progress=None):
        if lease_errors:
            raise RuntimeError("lease heartbeat failed") from lease_errors[0]
        db.heartbeat(job["id"], owner, progress)

    def keep_lease():
        while not stop.wait(db.settings.lease_seconds / 3):
            try:
                db.heartbeat(job["id"], owner)
            except Exception as exc:
                lease_errors.append(exc)
                return

    thread = threading.Thread(target=keep_lease, daemon=True)
    thread.start()
    try:
        result = execute(db, store, job, heartbeat)
        heartbeat({"result": result})
        db.finish(job["id"], owner)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"[:2000]
        db.finish(job["id"], owner, message)
        with db.connect() as con:
            if job["kind"] == "import":
                con.execute(
                    "UPDATE imports SET state='failed',error=%s WHERE id=%s AND state!='published'",
                    (message, job["payload"]["import_id"]),
                )
            if job["kind"] == "process_asset":
                con.execute(
                    "UPDATE assets SET state='failed',error=%s WHERE id=%s",
                    (message, job["payload"]["asset_id"]),
                )
    finally:
        stop.set()
        thread.join(timeout=5)
    return True


def run(db, store, *, once=False, drain=False):
    while True:
        found = run_once(db, store)
        if once or (drain and not found):
            return
        if not found:
            time.sleep(5)
