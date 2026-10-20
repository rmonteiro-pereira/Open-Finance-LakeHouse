"""Rechunk against concurrent passage writers, and saved investigations across re-cuts."""

import json
import threading
import time

import pytest

pytest.importorskip("psycopg")

from ofl.research import chunking
from ofl.research.archive import index_text, lock_item, rechunk, upsert_item
from ofl.research.library import index_news, sync_library
from ofl.research.search import search
from ofl.research.workspaces import load, save
from tests.test_research import research  # noqa: F401
from tests.test_workbench import LibraryFixture, news

GLUED = [f"demonstracoesfinanceir{i:04d}" for i in range(700)]
TEXT = " ".join(GLUED)
NEWS_KEY = "news/items/official-source/2026-01-06/120000.jsonl"


def legacy(monkeypatch):
    """Ingest as before the token bound: word-only windows."""
    windows = chunking.windows
    monkeypatch.setattr(chunking, "windows", lambda words, _=None: windows(words, None))
    return lambda: monkeypatch.setattr(chunking, "windows", windows)


def letter(db, monkeypatch, text=TEXT):
    restore = legacy(monkeypatch)
    with db.connect() as con:
        item = upsert_item(con, "manager_letter", {"source_id": "g", "title": "Carta"}, "g")
        index_text(con, item_id=item, revision="r", text=text, method="text", page=1)
    restore()
    return item


def active(db, item, method="text"):
    return db.rows(
        "SELECT id,revision,text FROM passages WHERE item_id=%s AND method=%s AND active",
        (item, method),
    )


def rechunk_blocked_by(db, con, item):
    """Start rechunk while `con` holds the item lock; return once it waits for it."""
    lock_item(con, item)
    outcome = {}
    worker = threading.Thread(target=lambda: outcome.update(rechunk(db)))
    worker.start()
    deadline = time.monotonic() + 20
    while not db.rows("SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted"):
        assert time.monotonic() < deadline and worker.is_alive(), "rechunk never waited"
        time.sleep(0.05)
    return worker, outcome


def test_rechunk_skips_a_chain_superseded_while_it_waited(research, monkeypatch):
    db, _ = research
    item = letter(db, monkeypatch)
    with db.connect() as con:
        worker, outcome = rechunk_blocked_by(db, con, item)
        index_text(
            con, item_id=item, revision="r2", text="Texto novo e curto", method="text", page=1
        )
    worker.join(20)
    assert not worker.is_alive()
    assert (outcome["rechunk"], outcome["changed"]) == (0, 1)
    assert [r["revision"] for r in active(db, item)] == ["r2"]
    # The superseded chain was never re-cut: no derived revision was written.
    assert not db.rows("SELECT 1 FROM passages WHERE method='text' AND revision NOT IN ('r', 'r2')")
    assert rechunk(db, dry_run=True)["rechunk"] == 0


def downgraded():
    row = news(body=" ".join(GLUED))
    source = LibraryFixture({NEWS_KEY: json.dumps(row).encode()})
    source.news_tiers = {"official-source": "headline_only"}
    return row, source


def full_news(db, store, monkeypatch):
    restore = legacy(monkeypatch)
    row = news(body=" ".join(GLUED))
    sync_library(db, store, "news", source=LibraryFixture({NEWS_KEY: json.dumps(row).encode()}))
    restore()
    [item] = db.rows("SELECT id FROM items WHERE source='news'")
    assert len(active(db, item["id"], "collector_news")) == 3
    return item["id"]


def body_visible(db):
    return db.rows(
        "SELECT 1 FROM passages WHERE active AND text LIKE %s", ("%" + GLUED[400] + "%",)
    )


def test_policy_downgrade_during_rechunk_never_reactivates_body_text(research, monkeypatch):
    db, store = research
    item = full_news(db, store, monkeypatch)
    row, source = downgraded()
    with db.connect() as con:
        worker, outcome = rechunk_blocked_by(db, con, item)
        later = {**row, "fetched_at": "2026-01-07T12:00:00Z"}
        assert index_news(con, db, store, later, NEWS_KEY, policies=source.news_tiers)
    worker.join(20)
    assert not worker.is_alive()
    assert (outcome["rechunk"], outcome["changed"]) == (0, 1)
    assert not body_visible(db)
    assert not search(db, GLUED[400], mode="keyword")["results"]


def test_policy_downgrade_after_rechunk_closes_the_recut_body(research, monkeypatch):
    db, store = research
    item = full_news(db, store, monkeypatch)
    assert rechunk(db)["rechunk"] == 1
    assert len(active(db, item, "collector_news")) > 3 and body_visible(db)
    _, source = downgraded()
    later = news(body=" ".join(GLUED), fetched="2026-01-07T12:00:00Z")
    source.blobs = {NEWS_KEY: json.dumps(later).encode()}
    sync_library(db, store, "news", source=source)
    assert not body_visible(db)
    assert rechunk(db)["rechunk"] == 0 and not body_visible(db)


def investigation(passage, quote, **extra):
    return {
        "question": "O que diz a carta?",
        "references": [passage],
        "findings": [{"kind": "observation", "text": quote, "references": [passage]}],
        "reference_reviews": [{"passage_id": passage, "verdict": "relevant", "rationale": "r"}],
        **extra,
    }


def test_saved_investigation_survives_rechunk_and_reingestion(research, monkeypatch):
    db, _ = research
    item = letter(db, monkeypatch)
    old = sorted(active(db, item), key=lambda r: r["text"].split()[0])
    first, second = old[0]["id"], old[1]["id"]
    saved = save(db, "owner", "investigation", "Tema", investigation(first, GLUED[5]))
    assert rechunk(db)["rechunk"] == 1
    record = load(db, "owner", saved["id"])
    body = record["body"]["content"]
    edit = {k: body[k] for k in ("question", "references", "findings", "reference_reviews")}
    edited = save(
        db,
        "owner",
        "investigation",
        "Tema revisto",
        edit,
        workspace_id=saved["id"],
        expected_revision=1,
    )
    again = load(db, "owner", saved["id"])
    assert again["body"]["title"] == "Tema revisto"
    assert again["body"]["content"]["evidence"] == body["evidence"]
    assert again["body"]["content"]["reference_reviews"] == body["reference_reviews"]
    # A new reference to a superseded passage is still refused.
    with pytest.raises(ValueError, match="^evidence reference unavailable$"):
        save(
            db,
            "owner",
            "investigation",
            "Tema",
            {**edit, "references": [first, second]},
            workspace_id=saved["id"],
            expected_revision=edited["revision"],
        )
    with db.connect() as con:
        index_text(con, item_id=item, revision="r3", text="Carta reemitida", method="text", page=1)
    assert (
        save(
            db,
            "owner",
            "investigation",
            "Tema",
            edit,
            workspace_id=saved["id"],
            expected_revision=2,
        )["revision"]
        == 3
    )


def test_restricted_news_reference_stays_blocked_on_edit(research, monkeypatch):
    db, store = research
    item = full_news(db, store, monkeypatch)
    [passage] = [r for r in active(db, item, "collector_news") if GLUED[400] in r["text"].split()][
        :1
    ]
    saved = save(db, "owner", "investigation", "Tema", investigation(passage["id"], GLUED[400]))
    _, source = downgraded()
    later = news(body=" ".join(GLUED), fetched="2026-01-07T12:00:00Z")
    source.blobs = {NEWS_KEY: json.dumps(later).encode()}
    sync_library(db, store, "news", source=source)
    body = load(db, "owner", saved["id"])["body"]["content"]
    assert passage["id"] in load(db, "owner", saved["id"])["redacted_references"]
    edit = {k: body[k] for k in ("question", "references", "findings", "reference_reviews")}
    with pytest.raises(ValueError, match="^evidence reference unavailable$"):
        save(
            db,
            "owner",
            "investigation",
            "Novo",
            edit,
            workspace_id=saved["id"],
            expected_revision=1,
        )


def waits_for_item_lock(db, item, write):
    """True when `write` (its own transaction) blocks while another one holds the item lock."""
    with db.connect() as con:
        lock_item(con, item)
        worker = threading.Thread(target=write)
        worker.start()
        deadline = time.monotonic() + 5
        while not db.rows("SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted"):
            if not worker.is_alive() or time.monotonic() > deadline:
                worker.join()
                return False
            time.sleep(0.05)
    worker.join(20)
    return True


def test_every_passage_writer_takes_the_item_lock(research, monkeypatch):
    db, store = research
    item = letter(db, monkeypatch)

    def reindex():
        with db.connect() as con:
            index_text(con, item_id=item, revision="r4", text="Outra", method="text", page=1)

    def recatalog():
        with db.connect() as con:
            upsert_item(con, "manager_letter", {"source_id": "g", "title": "Carta 2"}, "g")

    assert waits_for_item_lock(db, item, reindex)
    assert waits_for_item_lock(db, item, recatalog)
    news_item = full_news(db, store, monkeypatch)
    later = news(fetched="2026-01-08T12:00:00Z")
    source = LibraryFixture({NEWS_KEY: json.dumps(later).encode()})
    assert waits_for_item_lock(
        db, news_item, lambda: sync_library(db, store, "news", source=source)
    )
