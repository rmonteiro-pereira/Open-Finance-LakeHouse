"""Explicit transactions and PostgreSQL leases for research jobs."""

import hashlib
import json
import uuid
from contextlib import contextmanager
from importlib.resources import files

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .config import ResearchSettings


def uid() -> str:
    return uuid.uuid4().hex


def digest(value) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def lock_item(con, item_id):
    """Serialise every transaction that writes an item's passages (transaction-scoped).

    Taken before reading the passage state a write depends on: by upsert_item (catalog
    and archive ingestion, letter sync), news sync, page processing, index_text, the
    jurisdiction backfill and rechunk. An import or asset lock comes first and is never
    requested while an item lock is held. Rechunk and the backfill hold one item lock
    at a time and touch only that item's passages, so they do not wait for each other
    or for other passage writers on row locks. Residual risk: catalog imports and
    library sync take several item locks in one transaction, in no fixed order, so two
    of them could deadlock; PostgreSQL then aborts one and its job is retried.
    Re-acquiring the lock within the same transaction does not block.
    """
    con.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("ofl-research-item:" + item_id,))


class Database:
    def __init__(self, settings: ResearchSettings):
        self.settings = settings

    @contextmanager
    def connect(self):
        with psycopg.connect(
            self.settings.database_url, row_factory=dict_row, connect_timeout=5
        ) as con:
            yield con

    def migrate(self):
        with self.connect() as con:
            if con.execute("SELECT to_regclass('public.dag_run') AS airflow").fetchone()["airflow"]:
                raise ValueError("refusing to migrate an Airflow metadata database")
            try:
                con.execute(files("ofl.research").joinpath("schema.sql").read_text())
            except psycopg.errors.LockNotAvailable as error:
                raise RuntimeError(
                    "migration could not lock a research table within 5 s; another session "
                    "holds it (e.g. a running worker). Nothing was changed; retry when idle."
                ) from error
        # Keyword search reads the current lexical form only: every passage vector is
        # rewritten before the API starts. Batches commit, so a retry resumes.
        from . import lexical

        rebuilt = lexical.rebuild(self)
        if rebuilt["remaining"]:
            raise RuntimeError(
                f"{rebuilt['remaining']} passage vectors are not in {lexical.VERSION} yet "
                "(rows held by another session); retry the migration."
            )
        return rebuilt

    def rows(self, query, params=(), *, ann=False):
        with self.connect() as con:
            if ann:
                con.execute("SET LOCAL hnsw.iterative_scan='strict_order'")
            return con.execute(query, params).fetchall()

    def enqueue(self, kind, payload, actor="cli"):
        with self.connect() as con:
            return self.enqueue_on(con, kind, payload, actor)

    @staticmethod
    def enqueue_on(con, kind, payload, actor):
        job = uid()
        con.execute(
            "INSERT INTO jobs(id,kind,payload) VALUES (%s,%s,%s)", (job, kind, Jsonb(payload))
        )
        con.execute("INSERT INTO audit(actor,action,target) VALUES (%s,'enqueue',%s)", (actor, job))
        con.execute("INSERT INTO job_events(job_id,state) VALUES (%s,'queued')", (job,))
        return job

    def enqueue_if_idle(self, kind, payload, actor="cli"):
        """Avoid growing a recurring queue while the same kind is still in flight."""
        with self.connect() as con:
            con.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))", ("ofl-research-enqueue:" + kind,)
            )
            pending = con.execute(
                "SELECT id FROM jobs WHERE kind=%s AND state IN ('queued','running') ORDER BY created_at LIMIT 1",
                (kind,),
            ).fetchone()
            return pending["id"] if pending else self.enqueue_on(con, kind, payload, actor)

    def claim(self, owner, kinds=None):
        with self.connect() as con:
            if kinds is not None and not kinds:
                raise ValueError("worker kind selection cannot be empty")
            scope = " AND kind=ANY(%s)" if kinds is not None else ""
            row = con.execute(
                """SELECT id FROM jobs WHERE (state='queued' OR
                (state='running' AND lease_until < now()))"""
                + scope
                + """ ORDER BY created_at
                FOR UPDATE SKIP LOCKED LIMIT 1""",
                (list(kinds),) if kinds is not None else (),
            ).fetchone()
            if not row:
                return None
            claimed = con.execute(
                """UPDATE jobs SET state='running',lease_owner=%s,
                lease_until=now()+(%s * interval '1 second'),attempts=attempts+1,
                updated_at=now() WHERE id=%s RETURNING *""",
                (owner, self.settings.lease_seconds, row["id"]),
            ).fetchone()
            con.execute(
                "INSERT INTO job_events(job_id,state,detail) VALUES (%s,'running',%s)",
                (row["id"], Jsonb({"attempt": claimed["attempts"]})),
            )
            return claimed

    def heartbeat(self, job, owner, progress=None):
        with self.connect() as con:
            cur = con.execute(
                """UPDATE jobs SET lease_until=now()+(%s * interval '1 second'),
                progress=COALESCE(%s,progress),updated_at=now()
                WHERE id=%s AND lease_owner=%s AND state='running'""",
                (self.settings.lease_seconds, Jsonb(progress) if progress else None, job, owner),
            )
            if cur.rowcount != 1:
                raise RuntimeError("job lease lost")

    def finish(self, job, owner, error=None):
        with self.connect() as con:
            updated = con.execute(
                """UPDATE jobs SET state=%s,error=%s,lease_until=NULL,updated_at=now()
                WHERE id=%s AND lease_owner=%s AND state='running'""",
                ("failed" if error else "done", error, job, owner),
            )
            if updated.rowcount:
                con.execute(
                    "INSERT INTO job_events(job_id,state,detail) VALUES (%s,%s,%s)",
                    (job, "failed" if error else "done", Jsonb({"error": error})),
                )
