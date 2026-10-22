"""Explicit transactions and PostgreSQL leases for research jobs."""

import hashlib
import json
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from importlib.resources import files
from types import SimpleNamespace

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ofl.platform.logging import get_logger

from .config import ResearchSettings

log = get_logger(__name__)
# The longest wait between two connection attempts.
MAX_CONNECT_WAIT_SECONDS = 60.0


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
    def __init__(self, settings: ResearchSettings, *, sleep=time.sleep):
        self.settings = settings
        self._sleep = sleep
        # Set only inside session(): the connection connect() may borrow, its lock and
        # whether the session is still open.
        self._shared = ContextVar(f"research_session_{id(self)}", default=None)

    @contextmanager
    def connect(self):
        """One transaction: committed on exit, rolled back on error.

        Inside session() it borrows the session's connection when no other transaction
        holds it. A nested or concurrent connect() (another thread, or a task that
        copied this context) opens its own connection instead, so transactions never
        nest or interleave on the shared one."""
        shared = self._shared.get()
        if shared is not None and shared.lock.acquire(blocking=False):
            try:
                if shared.open:
                    with shared.connection.transaction():
                        yield shared.connection
                    return
            finally:
                shared.lock.release()
        with self._open() as con:
            yield con

    def _open(self, **options):
        """A new connection, retried while the server cannot be reached.

        Only opening the connection is retried. Nothing has been sent yet, so a retry
        cannot repeat a write; an error inside a transaction is never retried here."""
        attempts = self.settings.connect_attempts
        wait = self.settings.connect_retry_seconds
        for attempt in range(1, attempts):
            try:
                return self._connect_once(options)
            except psycopg.OperationalError as error:
                # The error type only: nothing from the connection string belongs in a log.
                log.warning(
                    "research_db_connect_retry",
                    attempt=attempt,
                    attempts=attempts,
                    wait_seconds=wait,
                    error=type(error).__name__,
                )
                self._sleep(wait)
                wait = min(wait * 2, MAX_CONNECT_WAIT_SECONDS)
        return self._connect_once(options)

    def _connect_once(self, options):
        return psycopg.connect(
            self.settings.database_url, row_factory=dict_row, connect_timeout=5, **options
        )

    @contextmanager
    def session(self):
        """Let every connect() in this block borrow one connection, each still its own
        transaction. A connection costs about a second on a starved host and one search
        makes several queries. Nested sessions reuse the outer one."""
        if self._shared.get() is not None:
            yield
            return
        with self._open(autocommit=True) as con:
            shared = SimpleNamespace(connection=con, lock=threading.Lock(), open=True)
            token = self._shared.set(shared)
            try:
                yield
            finally:
                self._shared.reset(token)
                # Wait for a borrower in a copied context; later ones see it closed.
                with shared.lock:
                    shared.open = False

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
