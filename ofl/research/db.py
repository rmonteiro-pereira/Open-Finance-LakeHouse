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
