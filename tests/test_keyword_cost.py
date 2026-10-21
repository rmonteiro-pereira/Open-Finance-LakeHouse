"""The cheaper keyword plan returns exactly what the previous one did, and the
per-process reuse (connection, coverage, query vectors) behaves as documented."""

import threading
import time
from types import SimpleNamespace

import pytest

from ofl.research import lexical
from ofl.research import search as searching
from ofl.research import vocabulary as lexicon
from ofl.research.archive import index_text, upsert_item
from ofl.research.search import PROJECTION, scope_conditions
from tests.test_research import research  # noqa: F401
from tests.test_vocabulary import seed

# Keyword SQL before item_headings, the coverage floor and the deferred projection.
PREVIOUS_KEYWORD = """WITH query AS (
                SELECT %s::tsquery AS strict, %s::tsquery AS ranked, %s::tsquery AS matched,
                    %s::text[]::tsquery[] AS operands, %s::float8 AS weight,
                    %s::tsquery AS expanded,
                    websearch_to_tsquery('{literal}',research_search_text(%s)) AS literal
                ), hits AS (
                SELECT p.id AS hit, p.item_id AS hit_item, p.search AS vector,
                    setweight(to_tsvector('{config}',
                        research_search_text(COALESCE(i.title,''))),'A') AS heading,
                    NOT p.search @@ query.matched AS literal_only
                FROM passages p JOIN items i ON i.id=p.item_id CROSS JOIN query
                WHERE {where} AND (p.search @@ query.matched OR
                    (p.method='metadata' AND strpos(query.literal::text,'!')=0
                    AND p.search @@ query.literal))
                ), coverage AS (
                SELECT h.hit_item, count(DISTINCT o.n) AS covered FROM hits h CROSS JOIN query
                CROSS JOIN LATERAL unnest(query.operands) WITH ORDINALITY AS o(operand,n)
                WHERE h.vector @@ o.operand OR h.heading @@ o.operand GROUP BY h.hit_item
                ), scored AS (
                SELECT DISTINCT ON (p.item_id) {projection},COALESCE(c.covered,0) AS covered,
                    COALESCE(c.covered,0)+0.9*r.strict/(1+r.strict)+0.1*r.loose/(1+r.loose) AS score
                FROM hits h JOIN passages p ON p.id=h.hit JOIN items i ON i.id=p.item_id
                CROSS JOIN query LEFT JOIN coverage c ON c.hit_item=h.hit_item
                CROSS JOIN LATERAL (SELECT strpos(query.literal::text,'!')=0
                    AND p.method='metadata' AS literal_rank) m
                CROSS JOIN LATERAL (SELECT ts_rank_cd(h.heading || h.vector, CASE WHEN m.literal_rank
                    THEN query.strict || query.literal ELSE query.strict END) AS strict,
                    ts_rank_cd(h.heading || h.vector, CASE WHEN m.literal_rank
                    THEN query.ranked || query.literal ELSE query.ranked END)
                    + query.weight*ts_rank_cd(h.heading || h.vector,query.expanded) AS loose) r
                WHERE COALESCE(c.covered,0) >= %s OR h.literal_only
                ORDER BY p.item_id,score DESC,p.id
                ) SELECT * FROM scored ORDER BY score DESC,id LIMIT %s"""

EXTRA = {
    "rates": ("Juros e câmbio no Brasil", "O dólar sobe e os juros caem."),
    "fed": ("Fed mantém juros americanos", "Inflação nos EUA e câmbio."),
    "china": ("Exportações da China", "O Brasil exporta mais para a China."),
    "tax": ("Reforma tributária avança", "Inflação, juros e câmbio no debate."),
}
QUERIES = (
    "inflação no Brasil",
    "juros americanos e Fed",
    "câmbio e dólar hoje",
    "reforma tributária",
    "exportações da China",
    "inflação -EUA",
    '"inflação brasileira" juros',
    "inflacao brasil juros cambio",
)


def seed_more(db):
    with db.connect() as con:
        for name, (title, body) in EXTRA.items():
            item = upsert_item(con, "news", {"source_id": name, "title": title}, name)
            con.execute("UPDATE passages SET active=false WHERE item_id=%s", (item,))
            for ordinal, text in enumerate((title, body)):
                index_text(
                    con,
                    item_id=item,
                    revision=f"{name}-{ordinal}",
                    text=text,
                    method="collector_news",
                )


def both(db, text, candidates, vocabulary="off", mode=None):
    expansion = lexicon.plan(vocabulary, text)
    arms = searching._arms(expansion, "keyword")
    terms = searching._keyword_terms(db, text, expansion, arms)
    where, params = scope_conditions()
    sql, head, minimum = searching._keyword_query(PROJECTION, where, text, terms, mode)
    current = db.rows(sql, (*head, *params, *searching._keyword_tail(minimum, candidates)))
    previous = db.rows(
        PREVIOUS_KEYWORD.format(
            projection=PROJECTION,
            where=where,
            config=lexical.CONFIG,
            literal=lexical.LITERAL,
        ),
        (*head, *params, minimum, candidates),
    )
    return current, previous


@pytest.mark.parametrize("candidates", [1, 2, 3, 100])
def test_keyword_plan_returns_the_previous_rows(research, candidates):  # noqa: F811
    db, _ = research
    seed(db)
    seed_more(db)
    for text in QUERIES:
        for vocabulary, mode in (("off", None), ("on", None), ("off", "ceil60")):
            current, previous = both(db, text, candidates, vocabulary, mode)
            assert current == previous, (text, vocabulary, mode)


def test_item_headings_follow_titles_and_a_missing_one_is_computed(research):  # noqa: F811
    db, _ = research
    ids = seed(db)
    seed_more(db)
    expression = searching.HEADING.format(config=lexical.CONFIG)
    stale = db.rows(
        f"""SELECT count(*) AS n FROM items i LEFT JOIN item_headings t ON t.item_id=i.id
        WHERE t.heading IS DISTINCT FROM {expression}"""
    )
    assert stale[0]["n"] == 0
    with db.connect() as con:
        con.execute("UPDATE items SET title='Câmbio e juros' WHERE id=%s", (ids["grant"],))
        con.execute("DELETE FROM item_headings WHERE item_id=%s", (ids["plain"],))
    title = db.rows(
        "SELECT heading::text AS h FROM item_headings WHERE item_id=%s", (ids["grant"],)
    )
    assert "'cambi'" in title[0]["h"] and "'jur'" in title[0]["h"]
    for text in QUERIES:
        current, previous = both(db, text, 100)
        assert current == previous, text
    # The next migration restores the missing heading.
    db.migrate()
    assert db.rows("SELECT 1 FROM item_headings WHERE item_id=%s", (ids["plain"],))


def test_session_lends_one_connection_with_one_transaction_per_call(research):  # noqa: F811
    db, _ = research
    with db.session():
        first = db.rows("SELECT pg_backend_pid() AS pid")[0]["pid"]
        with pytest.raises(Exception, match="division by zero"):
            db.rows("SELECT 1/0")
        with db.session():
            assert db.rows("SELECT pg_backend_pid() AS pid")[0]["pid"] == first
    assert db.rows("SELECT pg_backend_pid() AS pid")[0]["pid"] != first


class Counting:
    def __init__(self, ttl):
        self.settings = SimpleNamespace(
            coverage_cache_seconds=ttl,
            database_url=f"memory-{ttl}-{time.monotonic()}",
            embedding_model="m",
            embedding_revision="r",
        )
        self.calls = 0
        self.done = threading.Event()


def test_search_coverage_reuses_and_refreshes_in_the_background(monkeypatch):
    def scan(db, *, where=None, params=()):
        db.calls += 1
        db.done.set()
        return {"calls": db.calls}

    monkeypatch.setattr(searching, "index_coverage", scan)
    fresh = Counting(0)
    assert searching.search_coverage(fresh, "w", []) == {"calls": 1}
    assert searching.search_coverage(fresh, "w", []) == {"calls": 2}

    cached = Counting(60)
    assert searching.search_coverage(cached, "w", ["a"]) == {"calls": 1}
    assert searching.search_coverage(cached, "w", ["a"]) == {"calls": 1}
    assert searching.search_coverage(cached, "w", ["b"]) == {"calls": 2}
    # Past the lifetime: the old value now, the new one once the refresh lands.
    cached.done.clear()
    key = searching._coverage_key(cached, "w", ["a"])
    searching._COVERAGE[key] = (time.monotonic() - 61, {"calls": 1})
    assert searching.search_coverage(cached, "w", ["a"]) == {"calls": 1}
    assert cached.done.wait(5)
    for _ in range(100):
        if key not in searching._COVERAGE_REFRESHING:
            break
        time.sleep(0.01)
    assert searching.search_coverage(cached, "w", ["a"]) == {"calls": 3}


def test_query_vectors_are_reused_only_when_enabled(monkeypatch):
    calls = []

    def fake(settings, texts):
        calls.append(list(texts))
        return [[float(len(t)), 0.0] for t in texts]

    monkeypatch.setattr(searching, "embed", fake)
    off = SimpleNamespace(embedding_url="u", embedding_model="m", embedding_revision="r")
    assert searching._embed_queries(off, ["ab"]) == [[2.0, 0.0]]
    assert searching._embed_queries(off, ["ab"]) == [[2.0, 0.0]]
    assert calls == [["ab"], ["ab"]]

    calls.clear()
    on = SimpleNamespace(
        embedding_url="u", embedding_model="m", embedding_revision="r2", query_vector_cache=2
    )
    assert searching._embed_queries(on, ["ab", "abc"]) == [[2.0, 0.0], [3.0, 0.0]]
    assert searching._embed_queries(on, ["abc", "abcd"]) == [[3.0, 0.0], [4.0, 0.0]]
    assert calls == [["ab", "abc"], ["abcd"]]
    # Capacity 2: "ab" was the oldest and is embedded again.
    assert searching._embed_queries(on, ["ab"]) == [[2.0, 0.0]]
    assert calls[-1] == ["ab"]
