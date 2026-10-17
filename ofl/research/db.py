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
            con.execute(files("ofl.research").joinpath("schema.sql").read_text())

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
        return job

    def claim(self, owner):
        with self.connect() as con:
            row = con.execute("""SELECT id FROM jobs WHERE state='queued' OR
                (state='running' AND lease_until < now()) ORDER BY created_at
                FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
            if not row:
                return None
            return con.execute(
                """UPDATE jobs SET state='running',lease_owner=%s,
                lease_until=now()+(%s * interval '1 second'),attempts=attempts+1,
                updated_at=now() WHERE id=%s RETURNING *""",
                (owner, self.settings.lease_seconds, row["id"]),
            ).fetchone()

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
            con.execute(
                """UPDATE jobs SET state=%s,error=%s,lease_until=NULL,updated_at=now()
                WHERE id=%s AND lease_owner=%s AND state='running'""",
                ("failed" if error else "done", error, job, owner),
            )
