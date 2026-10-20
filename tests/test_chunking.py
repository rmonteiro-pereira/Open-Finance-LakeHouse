"""Token-bounded passage windows and the re-cut of passages chunked without the bound."""

import pytest

from ofl.research import chunking
from ofl.research import jurisdiction as places
from ofl.research.archive import chain_plan, chunks
from ofl.research.jurisdiction import tag
from tests.test_research import research  # noqa: F401

GLUED = "demonstracoesfinanceir"  # with a 4-digit suffix: 4 + 44 + 12 quarters = 15 tokens


def legacy(count):
    """The word-only windows passages were cut with before the token bound."""
    spans = []
    for start in range(0, count, 310):
        spans.append((start, min(start + 350, count)))
        if start + 350 >= count:
            break
    return spans


def glued(count, marks=None):
    words = [f"{GLUED}{i:04d}" for i in range(count)]
    for index, word in (marks or {}).items():
        words[index] = word
    return words


@pytest.mark.parametrize(
    ("word", "quarters"),
    [("", 4), ("ab", 8), ("A.", 8), ("12", 10), ("a1", 9), ("é", 8), ("²", 8), ("中文", 12)],
)
def test_word_cost_is_quarter_tokens_by_character_class(word, quarters):
    assert chunking.word_cost(word) == quarters


@pytest.mark.parametrize(("size", "shared"), [(350, 40), (349, 39), (175, 20), (9, 1), (8, 0)])
def test_overlap_is_proportional_and_forty_for_a_full_window(size, shared):
    assert chunking.overlap(size) == shared


@pytest.mark.parametrize("count", [0, 1, 40, 349, 350, 351, 660, 661, 700, 1001])
def test_text_within_budget_is_cut_exactly_as_before(count):
    words = [f"w{i}" for i in range(count)]
    assert chunking.windows(words) == chunking.windows(words, None) == legacy(count)
    assert list(chunks(" ".join(words))) == [" ".join(words[a:b]) for a, b in legacy(count)]


def test_budget_ends_a_window_at_the_last_word_that_fits():
    # "ab" costs 8 quarters: budget 4 tokens (16 quarters) holds exactly two.
    words = ["ab"] * 5
    assert chunking.windows(words, 4) == [(0, 2), (2, 4), (4, 5)]
    assert chunking.windows(words, 3) == [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]
    assert chunking.windows(["ab", "abc"], 4) == [(0, 1), (1, 2)]


def test_a_word_over_the_budget_is_kept_whole_in_its_own_window():
    words = ["a" * 400, "ab", "a" * 400]
    assert chunking.windows(words, 50) == [(0, 1), (1, 2), (2, 3)]
    assert list(chunks(" ".join(words), 50)) == words


def test_glued_text_windows_fit_the_budget_and_overlap_proportionally():
    words = glued(700)
    spans = chunking.windows(words)
    budget = chunking.TOKEN_BUDGET * 4
    assert spans[0] == (0, budget // chunking.word_cost(words[0]))
    for (a, b), (c, _) in zip(spans, spans[1:], strict=False):
        assert c == b - chunking.overlap(b - a) and c > a
    assert spans[-1][1] == 700
    assert all(sum(map(chunking.word_cost, words[a:b])) <= budget for a, b in spans)
    assert all(b - a <= chunking.WORDS for a, b in spans)
    assert chunking.windows(words, None) == legacy(700)


def test_document_rebuilds_bounded_and_word_only_chains_and_rejects_others():
    words = glued(700)
    bounded = list(chunks(" ".join(words)))
    before = list(chunks(" ".join(words), None))
    assert len(bounded) > len(before) == 3
    assert places.document(bounded) == words
    assert places.document(before) == words
    assert places.document([]) == []
    # Any other cut of the same words is not a chain chunking could have produced.
    assert places.document([bounded[0], *before[1:]]) is None
    assert places.document(bounded[:1] + bounded[2:]) is None
    assert places.document([bounded[0] + " extra", *bounded[1:]]) is None
    assert places.document([*bounded[:-1], bounded[-1] + " extra"]) == [*words, "extra"]
    assert places.passage_regions(range(0), []) == []


def test_related_region_crossing_a_bounded_boundary_keeps_document_context():
    marks = {130: "Leia", 131: "também:", 140: "EUA", 400: "Brasil", 401: "IPCA"}
    passages = list(chunks(" ".join(glued(500, marks))))
    assert chunking.windows(glued(500, marks))[:2] == [(0, 137), (122, 260)]
    regions = places.passage_regions(range(len(passages)), passages)
    assert {r["context"] for r in regions} == {"document"}
    first, second = (places._word_spans(p.split()) for p in passages[:2])
    # "Leia também:" (words 130-131) and the next 30 words, through word 161.
    assert regions[0]["related"] == [(first[130][0], first[136][1])]
    assert regions[1]["related"] == [(second[130 - 122][0], second[161 - 122][1])]
    assert regions[2]["related"] == []
    evidence = tag(passages[1], "", **regions[1])["evidence"]
    assert evidence["counts"]["US"]["strong"]["related"] == 1
    assert tag(passages[3], "", **regions[3])["jurisdiction"] == ["BR"]


def row(ordinal, text, **fields):
    base = {"method": "text", "asset_id": "a", "page": 1, "evidence": {"preview": "p"}}
    return {"ordinal": ordinal, "text": text, **base, **fields}


def test_chain_plan_rechunks_only_rebuildable_chains_cut_differently():
    words = glued(700)
    before = [row(i, t) for i, t in enumerate(chunks(" ".join(words), None))]
    bounded = [row(i, t) for i, t in enumerate(chunks(" ".join(words)))]
    short = [row(i, t) for i, t in enumerate(chunks(" ".join(f"w{i}" for i in range(700))))]
    assert chain_plan(before) == ("rechunk", words)
    assert chain_plan(bounded) == ("unchanged", None)
    assert chain_plan(short) == ("unchanged", None)
    assert chain_plan(before[1:]) == ("unchained", None)
    assert chain_plan([before[0], before[2]]) == ("unchained", None)
    for field, value in (("method", "ocr"), ("asset_id", "b"), ("page", 2), ("evidence", {})):
        mixed = [before[0] | {field: value}, *before[1:]]
        assert chain_plan(mixed) == ("unchained", None)
    single = [row(0, " ".join(glued(300)))]
    assert chain_plan(single) == ("rechunk", glued(300))


def fake_context_model(monkeypatch, db, max_chars=4500):
    """Remote model refusing long inputs, as Ollama does over its context with truncate=false."""
    import json

    import requests

    db.settings.embedding_url = "http://ollama.invalid"

    def reply(status, body):
        response = requests.Response()
        response.status_code = status
        response._content = json.dumps(body).encode()
        return response

    def post(_url, json, timeout):
        if any(len(text) > max_chars for text in json["input"]):
            return reply(413, {"error": "the input length exceeds the context length"})
        return reply(200, {"embeddings": [[1, 0, 0] for _ in json["input"]]})

    tags = {"models": [{"name": "test-model", "digest": "test-revision"}]}
    monkeypatch.setattr(requests, "get", lambda *_a, **_k: reply(200, tags))
    monkeypatch.setattr(requests, "post", post)


def counts(coverage):
    keys = ("eligible_passages", "indexed_passages", "rejected_passages", "pending_passages")
    return tuple(coverage[k] for k in keys) + (coverage["status"],)


def test_ingestion_keeps_identities_within_budget_and_recuts_glued_text(research):
    from ofl.research.archive import index_text, upsert_item
    from ofl.research.db import digest

    db, _ = research
    text = " ".join(glued(500))
    with db.connect() as con:
        item = upsert_item(con, "manager_letter", {"source_id": "g", "title": "Carta"}, "g")
        index_text(con, item_id=item, revision="r", text="Brasil e IPCA", method="text", page=1)
        index_text(con, item_id=item, revision="s", text=text, method="text", page=2)
    rows = db.rows("SELECT * FROM passages WHERE active AND method='text' ORDER BY page,ordinal")
    assert [(r["page"], r["revision"]) for r in rows[:2]] == [
        (1, "r"),
        (2, digest(["s", chunking.VERSION])),
    ]
    assert rows[0]["id"] == digest([item, "r", 0])
    assert [r["text"] for r in rows[1:]] == list(chunks(text))
    assert {r["jurisdiction_tagger"] for r in rows} == {places.VERSION}


def test_rechunk_supersedes_oversize_passages_and_their_rejections(research, monkeypatch):
    from ofl.research import archive
    from ofl.research.archive import index_text, rechunk, upsert_item
    from ofl.research.jurisdiction import backfill
    from ofl.research.search import embed_pending, index_coverage

    db, _ = research
    marks = {300: "Leia", 301: "também:", 310: "EUA", 600: "Brasil", 601: "IPCA"}
    text = " ".join(glued(700, marks))
    windows = chunking.windows
    with db.connect() as con:
        item = upsert_item(con, "manager_letter", {"source_id": "g", "title": "Carta"}, "g")
        plain = upsert_item(con, "manager_letter", {"source_id": "p", "title": "Outra"}, "p")
        # Passages cut before the token bound existed.
        monkeypatch.setattr(chunking, "windows", lambda words, _=None: windows(words, None))
        index_text(
            con,
            item_id=item,
            revision="r",
            text=text,
            method="text",
            page=3,
            evidence={"preview": "x"},
        )
        monkeypatch.setattr(chunking, "windows", windows)
    with db.connect() as con:
        index_text(con, item_id=plain, revision="p", text="Fed e FOMC", method="text", page=1)
    old = db.rows(
        "SELECT id,text FROM passages WHERE item_id=%s AND method='text' ORDER BY ordinal", (item,)
    )
    assert len(old) == 3 and all(len(r["text"]) > 4500 for r in old[:2])
    fake_context_model(monkeypatch, db)
    embed_pending(db, 100)
    # Three text windows (two over the model context), one plain passage, two metadata.
    assert counts(index_coverage(db)) == (6, 4, 2, 0, "partial")

    planned = rechunk(db, dry_run=True)
    assert planned == {
        "chunker": chunking.VERSION,
        "token_budget": chunking.TOKEN_BUDGET,
        "chains": 4,
        "rechunk": 1,
        "unchained": 0,
        "superseded_passages": 3,
        "new_passages": len(chunking.windows(text.split())),
        "superseded_rejections": 2,
        "dry_run": True,
    }
    assert db.rows("SELECT count(*) AS n FROM passages WHERE NOT active")[0]["n"] == 0
    seen = []
    assert rechunk(db, batch=1, progress=seen.append) == planned | {"dry_run": False}
    assert len(seen) == 4 and seen[-1] == planned | {"dry_run": False}
    assert rechunk(db, dry_run=True)["rechunk"] == 0
    new = db.rows(
        "SELECT * FROM passages WHERE item_id=%s AND active AND method='text' ORDER BY ordinal",
        (item,),
    )
    assert [r["text"] for r in new] == list(chunks(text))
    assert {(r["revision"], r["page"], r["method"]) for r in new} == {
        (archive.digest(["r", chunking.VERSION]), 3, "text")
    }
    assert {r["evidence"]["preview"] for r in new} == {"x"}
    assert {r["jurisdiction_evidence"]["context"] for r in new} == {"document"}
    assert backfill(db, dry_run=True)["pending"] == 0
    assert not db.rows(
        "SELECT 1 FROM passages WHERE id=ANY(%s) AND active", ([r["id"] for r in old],)
    )
    # Rejections stay recorded for the superseded passages but no longer count.
    assert len(db.rows("SELECT 1 FROM embedding_rejections")) == 2
    total = len(new) + 3
    assert counts(index_coverage(db)) == (total, 3, 0, len(new), "partial")
    embed_pending(db, 100)
    assert counts(index_coverage(db)) == (total, total, 0, 0, "complete")
