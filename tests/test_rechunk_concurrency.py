"""Rechunk against concurrent passage writers, and saved investigations across re-cuts."""

import json
import threading
import time
from contextlib import contextmanager

import pytest

pytest.importorskip("psycopg")

from ofl.research import chunking, jurisdiction, processing
from ofl.research.archive import index_text, rechunk, upsert_item
from ofl.research.db import lock_item
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


# Waiting on exactly this key: hashtext() widened to bigint, split as pg_locks stores it.
WAITING = """SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND objsubid=1
    AND pid=ANY(%s) AND classid::bigint=((hashtext(%s)::bigint >> 32) & 4294967295)
    AND objid::bigint=(hashtext(%s)::bigint & 4294967295)"""


class Writer:
    """Runs `write` in its own thread; records the backend of every connection it opens."""

    def __init__(self, db, monkeypatch, write):
        self.db, self.pids, self.error, self.result = db, [], None, None
        connect = db.connect

        @contextmanager
        def tracked():
            with connect() as con:
                if threading.get_ident() == self.thread.ident:
                    self.pids.append(con.info.backend_pid)
                yield con

        monkeypatch.setattr(db, "connect", tracked)

        def run():
            try:
                self.result = write()
            except BaseException as error:  # re-raised in the test thread by finish()
                self.error = error

        self.thread = threading.Thread(target=run)
        self.thread.start()

    def waits_for(self, item, timeout=20):
        """Assert this writer's own backend is queued on the item lock of `item`."""
        key = "ofl-research-item:" + item
        deadline = time.monotonic() + timeout
        while not self.db.rows(WAITING, (self.pids or [0], key, key)):
            if not self.thread.is_alive():
                self.finish()
                raise AssertionError("writer finished without waiting for the item lock")
            assert time.monotonic() < deadline, "writer never waited for the item lock"
            time.sleep(0.05)
        return self

    def finish(self, timeout=20):
        self.thread.join(timeout)
        assert not self.thread.is_alive(), "writer did not finish after the lock was released"
        if self.error:
            raise self.error
        return self.result


@contextmanager
def holding(db, item):
    """A transaction holding the item lock, as a concurrent passage writer would."""
    with db.connect() as con:
        lock_item(con, item)
        yield con


def rechunk_blocked_by(db, monkeypatch, item):
    return Writer(db, monkeypatch, lambda: rechunk(db)).waits_for(item)


def test_rechunk_skips_a_chain_superseded_while_it_waited(research, monkeypatch):
    db, _ = research
    item = letter(db, monkeypatch)
    with holding(db, item) as con:
        worker = rechunk_blocked_by(db, monkeypatch, item)
        index_text(
            con, item_id=item, revision="r2", text="Texto novo e curto", method="text", page=1
        )
    outcome = worker.finish()
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
    with holding(db, item) as con:
        worker = rechunk_blocked_by(db, monkeypatch, item)
        later = {**row, "fetched_at": "2026-01-07T12:00:00Z"}
        assert index_news(con, db, store, later, NEWS_KEY, policies=source.news_tiers)
    outcome = worker.finish()
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


def stale_tags(db, item):
    with db.connect() as con:
        con.execute(
            "UPDATE passages SET jurisdiction_tagger='jurisdiction-v0' WHERE item_id=%s", (item,)
        )


def tagger(db, item):
    return {
        r["t"]
        for r in db.rows(
            "SELECT jurisdiction_tagger AS t FROM passages WHERE item_id=%s AND active", (item,)
        )
    }


def test_backfill_rereads_a_chain_superseded_while_it_waited(research, monkeypatch):
    db, _ = research
    item = letter(db, monkeypatch)
    old = {r["id"] for r in active(db, item)}
    stale_tags(db, item)
    with holding(db, item) as con:
        worker = Writer(db, monkeypatch, lambda: jurisdiction.backfill(db)).waits_for(item)
        # What rechunk writes for this item, while the backfill waits for the lock.
        index_text(con, item_id=item, revision="r", text=TEXT, method="text", page=1)
    result = worker.finish()
    # The superseded passages were no longer pending under the lock: left untouched.
    rows = db.rows("SELECT id,jurisdiction_tagger FROM passages WHERE id=ANY(%s)", (list(old),))
    assert {r["jurisdiction_tagger"] for r in rows} == {"jurisdiction-v0"}
    assert tagger(db, item) == {jurisdiction.VERSION}
    assert result["tagged"] == 1  # the item's metadata passage, still current
    assert jurisdiction.backfill(db, dry_run=True)["pending"] == 0


def test_backfill_and_rechunk_on_one_item_both_complete(research, monkeypatch):
    db, _ = research
    item = letter(db, monkeypatch)
    stale_tags(db, item)
    with holding(db, item):
        tagging = Writer(db, monkeypatch, lambda: jurisdiction.backfill(db)).waits_for(item)
        recut = rechunk_blocked_by(db, monkeypatch, item)
    tagging.finish()
    assert recut.finish()["rechunk"] == 1
    assert rechunk(db, dry_run=True)["rechunk"] == 0
    assert tagger(db, item) == {jurisdiction.VERSION}
    assert jurisdiction.backfill(db, dry_run=True)["pending"] == 0


def test_every_passage_writer_waits_for_the_item_lock(research, monkeypatch, tmp_path):
    db, store = research
    item = letter(db, monkeypatch)

    def blocked(write, touch="SELECT 1"):
        with holding(db, item) as con:
            con.execute(touch)  # e.g. row locks a concurrent writer of the page holds
            worker = Writer(db, monkeypatch, write).waits_for(item)
        return worker.finish()

    def reindex():
        with db.connect() as con:
            index_text(con, item_id=item, revision="r4", text="Outra carta", method="text", page=1)

    blocked(reindex)
    assert [r["revision"] for r in active(db, item)] == ["r4"]

    def recatalog():
        with db.connect() as con:
            return upsert_item(con, "manager_letter", {"source_id": "g", "title": "Carta 2"}, "g")

    assert blocked(recatalog) == item
    assert db.rows("SELECT title FROM items WHERE id=%s", (item,)) == [{"title": "Carta 2"}]

    stale_tags(db, item)
    assert blocked(lambda: jurisdiction.backfill(db))["tagged"] == 2
    assert tagger(db, item) == {jurisdiction.VERSION}

    from PIL import Image

    image = tmp_path / "page.png"
    Image.new("RGB", (8, 8), "white").save(image)
    with db.connect() as con:
        con.execute(
            """INSERT INTO assets(id,item_id,sha256,object_key,name,media_type)
            VALUES ('page-asset',%s,'sha','k','page.png','image/png')""",
            (item,),
        )
    asset = {"id": "page-asset", "item_id": item, "sha256": "sha"}
    monkeypatch.setattr(processing, "ocr_image", lambda *_a, **_k: ("Página lida por OCR", []))
    monkeypatch.setattr(processing, "scan_tables", lambda *_a: iter(()))
    page = lambda: processing._page(db, store, asset, 1, image, None, lambda _: None)  # noqa: E731
    page()
    store.path("pages/page-asset/1/complete.json").unlink()  # process the page again
    # The holder also row-locks the page's passages: a page write that touched them
    # before taking the item lock would wait on those rows instead.
    blocked(page, "UPDATE passages SET active=true WHERE asset_id='page-asset'")
    assert db.rows("SELECT text,method FROM passages WHERE asset_id='page-asset' AND active") == [
        {"text": "Página lida por OCR", "method": "ocr"}
    ]

    news_item = full_news(db, store, monkeypatch)
    later = news(fetched="2026-01-08T12:00:00Z", body="Corpo curto")
    source = LibraryFixture({NEWS_KEY: json.dumps(later).encode()})
    with holding(db, news_item):
        worker = Writer(
            db, monkeypatch, lambda: sync_library(db, store, "news", source=source)
        ).waits_for(news_item)
    assert worker.finish()["items"] == 1
    assert any("Corpo curto" in r["text"] for r in active(db, news_item, "collector_news"))
