"""Opening research database connections while PostgreSQL restarts."""

import psycopg
import pytest
from pydantic import ValidationError
from structlog.testing import capture_logs

from ofl.research import db as research_db
from ofl.research.config import ResearchSettings
from ofl.research.db import Database

SECRET_URL = "postgresql://research:not-a-real-password@db.example:5432/research"


class FakeConnection:
    """Stands in for a psycopg connection; records whether it was closed."""

    def __init__(self, options):
        self.options = options
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.closed = True

    def transaction(self):
        return self


class Server:
    """Refuses the first `refusals` connections, then accepts every one."""

    def __init__(self, refusals):
        self.refusals = refusals
        self.calls = []

    def connect(self, url, **options):
        self.calls.append(options)
        if len(self.calls) <= self.refusals:
            # A real message names the host; this one carries the whole connection
            # string, so a log that copied the message would leak the password.
            raise psycopg.OperationalError(f"connection to {url} failed: shutting down")
        return FakeConnection(options)


def database(monkeypatch, refusals, **settings):
    server = Server(refusals)
    monkeypatch.setattr(research_db.psycopg, "connect", server.connect)
    waits = []
    values = {"database_url": SECRET_URL, **settings}
    return Database(ResearchSettings(**values), sleep=waits.append), server, waits


def test_connection_is_retried_until_the_server_accepts_it(monkeypatch):
    db, server, waits = database(monkeypatch, refusals=2)
    with db.connect() as con:
        assert isinstance(con, FakeConnection)
    assert waits == [5.0, 10.0]
    assert len(server.calls) == len(waits) + 1


def test_retries_stop_after_the_configured_attempts(monkeypatch):
    db, server, waits = database(
        monkeypatch, refusals=99, connect_attempts=7, connect_retry_seconds=20
    )
    with pytest.raises(psycopg.OperationalError), db.connect():
        pass
    assert len(server.calls) == db.settings.connect_attempts
    # Waits double and stop growing at one minute.
    assert waits == [20.0, 40.0, 60.0, 60.0, 60.0, 60.0]


def test_one_attempt_turns_retrying_off(monkeypatch):
    db, server, waits = database(monkeypatch, refusals=1, connect_attempts=1)
    with pytest.raises(psycopg.OperationalError), db.connect():
        pass
    assert len(server.calls) == 1
    assert waits == []


def test_an_error_inside_the_transaction_is_never_retried(monkeypatch):
    db, server, waits = database(monkeypatch, refusals=0)
    with pytest.raises(psycopg.OperationalError, match="lost mid-write"), db.connect():
        raise psycopg.OperationalError("connection lost mid-write")
    assert len(server.calls) == 1
    assert waits == []


def test_an_error_inside_a_session_transaction_is_never_retried(monkeypatch):
    db, server, waits = database(monkeypatch, refusals=0)
    with pytest.raises(psycopg.OperationalError, match="lost mid-write"), db.session():
        with db.connect():
            raise psycopg.OperationalError("connection lost mid-write")
    assert len(server.calls) == 1
    assert waits == []


def test_shared_session_connection_is_retried_too(monkeypatch):
    db, server, waits = database(monkeypatch, refusals=1)
    with db.session(), db.connect() as con:
        assert con.options["autocommit"] is True
    assert waits == [5.0]
    assert len(server.calls) == len(waits) + 1


def test_retry_log_never_holds_the_connection_string(monkeypatch):
    db, _, _ = database(monkeypatch, refusals=1)
    with capture_logs() as logs, db.connect():
        pass
    assert [entry["event"] for entry in logs] == ["research_db_connect_retry"]
    assert logs[0]["error"] == "OperationalError"
    assert "not-a-real-password" not in repr(logs)
    assert "db.example" not in repr(logs)


def test_retry_budget_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("OFL_RESEARCH_CONNECT_ATTEMPTS", "3")
    monkeypatch.setenv("OFL_RESEARCH_CONNECT_RETRY_SECONDS", "0.5")
    settings = ResearchSettings()
    assert (settings.connect_attempts, settings.connect_retry_seconds) == (3, 0.5)
    monkeypatch.setenv("OFL_RESEARCH_CONNECT_ATTEMPTS", "0")
    with pytest.raises(ValidationError):
        ResearchSettings()
