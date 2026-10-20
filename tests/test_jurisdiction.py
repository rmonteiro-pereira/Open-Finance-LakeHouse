"""Jurisdiction evidence: tagger, query detection, rule variants and the hand-check audit."""

import asyncio
import json
import time
from pathlib import Path

import pytest

pytest.importorskip("psycopg")

from ofl.research import jurisdiction as places
from ofl.research.jurisdiction import (
    boosted,
    check_rule,
    detect,
    exclusion,
    find,
    related_spans,
    tag,
)

ROOT = Path(__file__).resolve().parent.parent


def audit_tool():
    from ofl.research import jurisdiction_audit

    return jurisdiction_audit


def codes(text, title=""):
    return tag(text, title)["jurisdiction"]


@pytest.mark.parametrize(
    "text,expected",
    [
        ("O IPCA de março subiu.", ["BR"]),
        ("A Selic ficou estável após o Copom.", ["BR"]),
        ("Brasileiros pagam mais.", ["BR"]),
        ("Preço de R$100 no varejo.", ["BR"]),
        ("Juros nos Estados Unidos sobem.", ["US"]),
        ("O Fed manteve os juros; o FOMC se reúne.", ["US"]),
        ("Ações norte-americanas caem em Wall Street.", ["US"]),
        ("Juros americanos pesam.", ["US"]),
        ("Inflação na zona do euro e no Banco Central Europeu.", ["EA"]),
        ("O BCE decidiu.", ["EA"]),
        ("Exportações chinesas para Pequim.", ["CN"]),
        ("O yuan se valorizou.", ["CN"]),
        ("Inflação no Brasil e nos EUA.", ["BR", "US"]),
        ("Inflação de serviços desacelera.", []),
    ],
)
def test_strong_markers_tag_passages(text, expected):
    assert codes(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "O real se desvalorizou frente ao dólar.",
        "Juros reais altos.",
        "O CPI subiu 0,3%.",
        "Treasuries e payroll movem o mercado.",
        "O Banco Central elevou os juros.",
        "O Tesouro vendeu títulos.",
        "O euro caiu na Europa.",
        "Ativos de US$ 5 bilhões.",
        "Mercados latino-americanos e sul-americanos.",
        "A fed-funds curve.",
        "Uma notícia escrita em português sobre inflação e juros.",
        "fed e bce em minúsculas não são siglas.",
        "Indochina e chinelos.",
    ],
)
def test_ambiguous_markers_and_language_never_tag(text):
    assert codes(text) == []


def test_weak_markers_are_recorded_but_do_not_support_a_tag():
    result = tag("O real caiu e o CPI subiu.")
    assert result["jurisdiction"] == []
    assert result["evidence"]["counts"] == {
        "BR": {"title": 0, "body": 0, "related": 0, "weak": 1},
        "US": {"title": 0, "body": 0, "related": 0, "weak": 1},
    }
    assert [(m["term"], m["strength"], m["where"]) for m in result["evidence"]["matches"]] == [
        ("real", "weak", "body"),
        ("CPI", "weak", "body"),
    ]


def test_strong_longer_matches_mask_weak_and_overlapping_markers():
    result = tag("O Banco Central Europeu e a zona do euro; o Tesouro Nacional.")
    assert result["jurisdiction"] == ["BR", "EA"]
    terms = [(m["term"], m["code"], m["strength"]) for m in result["evidence"]["matches"]]
    assert terms == [
        ("Banco Central Europeu", "EA", "strong"),
        ("zona do euro", "EA", "strong"),
        ("Tesouro Nacional", "BR", "strong"),
    ]
    assert [(s, e, c) for s, e, c, _ in find("Banco Central do Brasil")] == [
        (0, 13, "BR"),
        (17, 23, "BR"),
    ]
    assert [m[3] for m in find("Banco Central do Brasil")] == ["weak", "strong"]


def test_related_link_and_navigation_text_never_tags():
    text = (
        "Inflação de serviços desacelera no Brasil. Leia também: Inflação nos EUA dispara "
        "e Fed reage. Tags: China, zona do euro"
    )
    result = tag(text)
    assert result["jurisdiction"] == ["BR"]
    counts = result["evidence"]["counts"]
    assert counts["US"] == {"title": 0, "body": 0, "related": 2, "weak": 0}
    assert counts["CN"]["related"] == 1 and counts["EA"]["related"] == 1
    only_related = tag("Mercados. LEIA MAIS: Estados Unidos e China em guerra comercial.")
    assert only_related["jurisdiction"] == []
    assert tag("Veja também: EUA. O IPCA subiu.")["jurisdiction"] == ["BR"]


def test_related_region_ends_at_sentence_or_word_bound():
    assert related_spans("Leia também: EUA sobem. Brasil cai.") == [(0, 23)]
    words = " ".join(f"w{i}" for i in range(40))
    text = f"Saiba mais {words} Brasil"
    ((start, end),) = related_spans(text)
    assert start == 0 and text[end - 3 : end] == "w29"
    assert codes(text) == ["BR"]
    assert related_spans("Leia também EUA") == [(0, 15)]
    assert related_spans("Sem marcador. EUA.") == []
    assert related_spans("Releia também os EUA.") == []
    words29 = " ".join(f"w{i}" for i in range(29))
    assert related_spans(f"Saiba mais {words29}") == [(0, len(f"Saiba mais {words29}"))]
    words30 = " ".join(f"w{i}" for i in range(30))
    assert related_spans(f"Saiba mais {words30}   ") == [(0, len(f"Saiba mais {words30}"))]


def test_overlaps_prefer_strong_then_longer_then_earlier_and_allow_adjacency(monkeypatch):
    import re

    def patterns(*entries):
        return [(code, strength, re.compile(p)) for code, strength, p in entries]

    monkeypatch.setattr(
        places, "PATTERNS", patterns(("US", "weak", "abcdef"), ("BR", "strong", "cd"))
    )
    assert find("abcdef") == [(2, 4, "BR", "strong")]
    monkeypatch.setattr(
        places, "PATTERNS", patterns(("US", "strong", "ab"), ("BR", "strong", "abcd"))
    )
    assert find("abcd") == [(0, 4, "BR", "strong")]
    monkeypatch.setattr(
        places, "PATTERNS", patterns(("US", "strong", "bcd"), ("BR", "strong", "abc"))
    )
    assert find("abcd") == [(0, 3, "BR", "strong")]
    monkeypatch.setattr(
        places, "PATTERNS", patterns(("BR", "strong", "ab"), ("US", "strong", "cd"))
    )
    assert find("abcd") == [(0, 2, "BR", "strong"), (2, 4, "US", "strong")]
    monkeypatch.setattr(
        places, "PATTERNS", patterns(("US", "strong", "cd"), ("BR", "strong", "ab"))
    )
    assert find("abcd") == [(0, 2, "BR", "strong"), (2, 4, "US", "strong")]


def test_title_is_counted_once_and_inherited_by_every_passage():
    title = "Inflação  nos EUA"
    first = tag("Inflação nos EUA surpreende os analistas.", title)
    assert first["jurisdiction"] == ["US"]
    assert first["evidence"]["counts"]["US"] == {"title": 1, "body": 0, "related": 0, "weak": 0}
    assert first["evidence"]["matches"] == [
        {"code": "US", "term": "EUA", "where": "title", "strength": "strong", "offset": 14}
    ]
    later = tag("O índice de serviços subiu.", title)
    assert later["jurisdiction"] == ["US"]
    assert later["evidence"]["tagger"] == places.VERSION
    import hashlib

    assert later["evidence"]["title_sha256"] == hashlib.sha256(title.encode()).hexdigest()
    assert tag("EUA", "")["evidence"]["title_sha256"] == hashlib.sha256(b"").hexdigest()
    assert tag("EUA")["evidence"]["title_sha256"] == hashlib.sha256(b"").hexdigest()
    assert tag("O IPCA sobe.", "Selic")["evidence"]["counts"]["BR"]["body"] == 1
    assert tag("Brasil cresce. Brasil", "Brasil")["jurisdiction"] == ["BR"]
    assert tag("Brasil cresce. Brasil", "Brasil")["evidence"]["counts"]["BR"] == {
        "title": 1,
        "body": 1,
        "related": 0,
        "weak": 0,
    }
    assert tag("Texto sem título", None)["jurisdiction"] == []


def test_match_list_is_bounded_but_counts_are_complete():
    result = tag("Brasil " * 60)
    assert len(result["evidence"]["matches"]) == places.MAX_MATCHES
    assert result["evidence"]["counts"]["BR"]["body"] == 60


@pytest.mark.parametrize(
    "query,expected",
    [
        ("inflação Brasil", ["BR"]),
        ("inflação brasileira", ["BR"]),
        ("IPCA acumulado em 12 meses", ["BR"]),
        ("juros nos Estados Unidos", ["US"]),
        ("custo de vida nos EUA", ["US"]),
        ("inflação na zona do euro", ["EA"]),
        ("inflação na China", ["CN"]),
        ("Selic e juros americanos comparados", ["BR", "US"]),
        ("juros americanos e impacto nos ativos brasileiros", ["BR", "US"]),
        ("inflação", []),
        ("taxa de juros", []),
        ("política monetária do Banco Central", []),
        ("câmbio dólar real", []),
        ("receita de bolo de chocolate", []),
        ("CPI e treasuries", []),
        ("inflação -Brasil", []),
        ("-Brasil", []),
        ("inflação -EUA Brasil", ["BR"]),
        ("latino-americanos", []),
        ("Brasil-EUA", ["BR", "US"]),
    ],
)
def test_query_detection_fires_only_on_explicit_strong_names(query, expected):
    assert detect(query) == expected


def test_rules_are_validated():
    assert [check_rule(rule) for rule in ("off", "boost", "exclude")] == ["off", "boost", "exclude"]
    for rule in ("", "OFF", "filter", None):
        with pytest.raises(ValueError, match="^jurisdiction rule must be off, boost or exclude$"):
            check_rule(rule)


def test_exclusion_keeps_untagged_unknown_named_and_mixed():
    sql, params = exclusion(["BR"])
    assert params == [["BR"], ["BR"]]
    assert sql == (
        "(p.jurisdiction IS NULL OR p.jurisdiction <@ %s::text[] OR p.jurisdiction && %s::text[])"
    )

    def kept(tags, named):
        # Python mirror of the SQL predicate for the documented truth table.
        return tags is None or set(tags) <= set(named) or bool(set(tags) & set(named))

    table = {
        None: True,
        (): True,
        ("BR",): True,
        ("BR", "US"): True,
        ("US",): False,
        ("CN", "US"): False,
    }
    assert {tags: kept(tags, ["BR"]) for tags in table} == table


def test_boost_list_keeps_base_order_and_only_named_evidence():
    rows = [
        {"id": "a", "jurisdiction": ["US"]},
        {"id": "b", "jurisdiction": ["BR", "US"]},
        {"id": "c", "jurisdiction": None},
        {"id": "d", "jurisdiction": ["BR"]},
        {"id": "e"},
        {"id": "f", "jurisdiction": ["CN"]},
    ]
    assert [r["id"] for r in boosted(rows, ["BR"])] == ["b", "d"]
    assert [r["id"] for r in boosted(rows, ["BR", "CN"])] == ["b", "d", "f"]
    assert boosted(rows, []) == []


def test_audit_strata_quotas_and_precision():
    audit = audit_tool()
    assert [
        audit.stratum(c) for c in (["BR"], ["US"], ["BR", "US", "CN"], [], ["EA"], ["BR", "EA"])
    ] == [
        "BR",
        "US",
        "both",
        "none",
        "other",
        "BR",
    ]
    assert audit.quotas(100) == dict.fromkeys(audit.STRATA, 20)
    assert audit.quotas(7) == {"BR": 2, "US": 2, "both": 1, "none": 1, "other": 1}
    from ofl.research.db import digest

    def row(tags, verdicts, missed=(), status="assessed"):
        captured = {
            "passage_id": str(tags),
            "item_id": "i",
            "source": "news",
            "title": "t",
            "text": "private",
            "stratum": audit.stratum(tags),
            "jurisdiction": tags,
            "evidence": {},
        }
        return {
            **captured,
            "capture_sha256": digest(captured),
            "review": {
                "status": status,
                "tags": dict(zip(tags, verdicts, strict=True)),
                "missed": list(missed),
                "note": "",
            },
        }

    sheet = {
        "version": 1,
        "tagger": places.VERSION,
        "rows": [
            row(["BR"], ["correct"]),
            row(["BR"], ["incorrect"]),
            row(["BR"], ["uncertain"]),
            row(["BR"], ["not_assessed"], status="not_assessed"),
            row(["BR", "US"], ["correct", "correct"], missed=["CN"]),
            row([], [], missed=["BR"]),
            row([], []),
        ],
    }
    report = audit.assess(sheet)
    assert report["strata"]["BR"] == {
        "rows": 4,
        "assessed_rows": 3,
        "correct": 1,
        "incorrect": 1,
        "uncertain": 1,
        "rows_with_missed_tags": 0,
        "tag_precision": 0.5,
        "missed_rate": 0.0,
    }
    assert report["strata"]["both"]["tag_precision"] == 1.0
    assert report["strata"]["both"]["missed_rate"] == 1.0
    assert report["strata"]["none"]["tag_precision"] is None
    assert report["strata"]["none"]["missed_rate"] == 0.5
    assert report["strata"]["other"]["missed_rate"] is None
    assert report["worksheet_sha256"] == digest(sheet) and "private" not in json.dumps(report)
    for mutate in (
        lambda s: s["rows"][0].update(text="edited"),
        lambda s: s["rows"][0]["review"].update(status="done"),
        lambda s: s["rows"][0]["review"]["tags"].update(BR="maybe"),
        lambda s: s["rows"][0]["review"]["tags"].update(US="correct"),
        lambda s: s["rows"][0]["review"].update(missed=["BR"]),
        lambda s: s["rows"][0]["review"].update(missed=["XX"]),
        lambda s: s["rows"][0]["review"].update(missed="CN"),
        lambda s: s["rows"][0]["review"].update(tags=[]),
        lambda s: s["rows"][0]["review"]["tags"].update(BR="not_assessed"),
        lambda s: s.update(version=2),
        lambda s: s.update(rows={}),
    ):
        broken = json.loads(json.dumps(sheet))
        mutate(broken)
        with pytest.raises(ValueError):
            audit.assess(broken)
    forged = json.loads(json.dumps(sheet))
    forged["rows"][0]["stratum"] = "US"
    captured = {k: forged["rows"][0][k] for k in audit.CAPTURED}
    forged["rows"][0]["capture_sha256"] = digest(captured)
    with pytest.raises(ValueError, match="stratum"):
        audit.assess(forged)


def test_mcp_validates_and_forwards_the_jurisdiction_rule():
    httpx = pytest.importorskip("httpx")
    pytest.importorskip("mcp")
    from ofl.research.config import ResearchSettings
    from ofl.research.mcp import create_mcp

    seen = []

    def source(request):
        seen.append(dict(request.url.params))
        return httpx.Response(200, json={"results": []})

    server = create_mcp(ResearchSettings(api_token="t" * 40), transport=httpx.MockTransport(source))

    async def exercise():
        await server.call_tool("search_archive", {"q": "inflação Brasil"})
        await server.call_tool(
            "search_archive", {"q": "inflação Brasil", "jurisdiction": "exclude"}
        )
        with pytest.raises(Exception):
            await server.call_tool("search_archive", {"q": "inflação", "jurisdiction": "only"})

    asyncio.run(exercise())
    assert "jurisdiction" not in seen[0] and seen[1]["jurisdiction"] == "exclude"
    assert len(seen) == 2


# PostgreSQL contracts (disposable schema per test; see tests/test_research.py).
from tests.test_research import research  # noqa: E402,F401

CORPUS = {
    "br": ("IPCA sobe e inflação no Brasil acelera", "A inflação de alimentos pressiona o índice."),
    "mixed": ("Inflação nos EUA surpreende", "A inflação americana assusta; o Brasil acompanha."),
    "us": ("Inflação americana: CPI sobe", "O Fed vê inflação persistente nos Estados Unidos."),
    "cn": ("Inflação na China cai", "A inflação chinesa recua."),
    "related": (
        "Inflação de serviços",
        "Inflação de serviços desacelera. Leia também: inflação nos EUA dispara.",
    ),
    "neutral": ("Inflação global", "A inflação global desacelera."),
    "untagged": ("Inflação nos EUA", "A inflação dos EUA sobe."),
}
VECTORS = {
    "br": "[0.6,0.8,0]",
    "mixed": "[0.8,0.6,0]",
    "us": "[1,0,0]",
    "cn": "[0.99,0.1,0]",
    "related": "[0.5,0.5,0.7]",
    "neutral": "[0,1,0]",
    "untagged": "[0.9,0.4,0]",
}


def seed(db):
    from ofl.research.archive import index_text, upsert_item

    ids = {}
    with db.connect() as con:
        for name, (title, body) in CORPUS.items():
            item = upsert_item(con, "news", {"source_id": name, "title": title}, name)
            con.execute("UPDATE passages SET active=false WHERE item_id=%s", (item,))
            index_text(
                con, item_id=item, revision=name, text=f"{title}\n{body}", method="collector_news"
            )
            passage = con.execute(
                "SELECT id FROM passages WHERE item_id=%s AND active", (item,)
            ).fetchone()["id"]
            con.execute(
                "INSERT INTO embeddings VALUES (%s,%s,%s,%s::vector)",
                (
                    passage,
                    db.settings.embedding_model,
                    db.settings.embedding_revision,
                    VECTORS[name],
                ),
            )
            ids[name] = item
        con.execute(
            """UPDATE passages SET jurisdiction=NULL,jurisdiction_evidence=NULL,jurisdiction_tagger=NULL
            WHERE item_id=%s""",
            (ids["untagged"],),
        )
    return ids


def names(result, ids):
    reverse = {v: k for k, v in ids.items()}
    return [reverse[row["item_id"]] for row in result["results"]]


def test_ingestion_tags_passages_and_backfill_never_reembeds(research):
    db, _ = research
    ids = seed(db)
    tags = {
        r["item_id"]: r["jurisdiction"]
        for r in db.rows("SELECT item_id,jurisdiction FROM passages WHERE active")
    }
    assert {name: tags[item] for name, item in ids.items()} == {
        "br": ["BR"],
        "mixed": ["BR", "US"],
        "us": ["US"],
        "cn": ["CN"],
        "related": [],
        "neutral": [],
        "untagged": None,
    }
    from ofl.research.jurisdiction import backfill

    embeddings = db.rows("SELECT passage_id,embedding::text FROM embeddings ORDER BY 1")
    assert backfill(db, dry_run=True) == {"tagger": places.VERSION, "pending": 1, "tagged": 0}
    seen = []
    assert backfill(db, batch=1, progress=seen.append) == {
        "tagger": places.VERSION,
        "pending": 1,
        "tagged": 1,
    }
    assert seen == [{"tagger": places.VERSION, "pending": 1, "tagged": 1}]
    assert backfill(db)["pending"] == 0
    with db.connect() as con:
        con.execute("UPDATE passages SET jurisdiction_tagger='jurisdiction-v0'")
        con.execute(
            "UPDATE items SET title='Inflação na zona do euro' WHERE id=%s", (ids["neutral"],)
        )
    assert backfill(db, batch=2)["tagged"] == len(ids)
    rows = {r["item_id"]: r for r in db.rows("SELECT * FROM passages WHERE active")}
    assert rows[ids["untagged"]]["jurisdiction"] == ["US"]
    assert rows[ids["neutral"]]["jurisdiction"] == ["EA"]
    assert rows[ids["br"]]["jurisdiction_evidence"]["counts"]["BR"]["title"] == 2
    assert {r["jurisdiction_tagger"] for r in rows.values()} == {places.VERSION}
    assert db.rows("SELECT passage_id,embedding::text FROM embeddings ORDER BY 1") == embeddings
    with db.connect() as con:
        con.execute("UPDATE items SET title='Outro título' WHERE id=%s", (ids["br"],))
    assert backfill(db, dry_run=True)["pending"] == 1


@pytest.mark.parametrize("mode", ["keyword", "vector", "hybrid"])
def test_off_is_identical_and_neutral_queries_never_fire(research, mode):
    from ofl.research.search import search

    db, _ = research
    seed(db)
    query, vector = "inflação", [1, 0, 0]
    base = search(db, query, mode=mode, vector=vector)
    assert search(db, query, mode=mode, vector=vector, jurisdiction="off") == base
    assert "jurisdiction" not in base and "jurisdiction" not in base["results"][0]
    with db.connect() as con:
        con.execute("UPDATE passages SET jurisdiction=NULL,jurisdiction_tagger=NULL")
    assert search(db, query, mode=mode, vector=vector) == base
    for rule in ("boost", "exclude"):
        neutral = search(db, query, mode=mode, vector=vector, jurisdiction=rule)
        assert [r["id"] for r in neutral["results"]] == [r["id"] for r in base["results"]]
        assert [r["score"] for r in neutral["results"]] == [r["score"] for r in base["results"]]
        assert (
            neutral["jurisdiction"]["applied"] is False and neutral["jurisdiction"]["named"] == []
        )
        assert "excluded_passages" not in neutral["jurisdiction"]
    with pytest.raises(ValueError, match="off, boost or exclude"):
        search(db, query, mode=mode, vector=vector, jurisdiction="filter")


@pytest.mark.parametrize("mode", ["keyword", "vector", "hybrid"])
def test_exclude_drops_only_other_jurisdiction_passages_in_every_arm(research, mode):
    from ofl.research.search import search

    db, _ = research
    ids = seed(db)
    query, vector = "inflação OR Brasil", [1, 0, 0]
    off = search(db, query, mode=mode, vector=vector, limit=50)
    assert set(names(off, ids)) == set(CORPUS)
    result = search(db, query, mode=mode, vector=vector, limit=50, jurisdiction="exclude")
    assert set(names(result, ids)) == set(CORPUS) - {"us", "cn"}
    disclosure = result["jurisdiction"]
    assert disclosure | {"qualification": None} == {
        "rule": "exclude",
        "tagger": places.VERSION,
        "named": ["BR"],
        "applied": True,
        "qualification": None,
        "scope_passages": 7,
        "passages_with_named_evidence": 2,
        "conflicting_passages": 2,
        "excluded_passages": 2,
        "untagged_passages": 1,
        "older_tagger_passages": 0,
    }
    if mode != "keyword":
        assert result["index_coverage"]["eligible_passages"] == 5
    row = next(r for r in result["results"] if r["item_id"] == ids["mixed"])
    assert row["jurisdiction"] == ["BR", "US"] and row["jurisdiction_tagger"] == places.VERSION
    assert {m["where"] for m in row["jurisdiction_evidence"]["matches"]} == {"title", "body"}


@pytest.mark.parametrize("mode", ["keyword", "vector", "hybrid"])
def test_boost_is_one_bounded_extra_fused_list(research, mode):
    from ofl.research.search import search

    db, _ = research
    ids = seed(db)
    query, vector = "inflação OR Brasil", [1, 0, 0]
    off = search(db, query, mode=mode, vector=vector, limit=50)
    result = search(db, query, mode=mode, vector=vector, limit=50, jurisdiction="boost")
    assert set(names(result, ids)) == set(names(off, ids))
    if mode != "hybrid":
        # Seven candidates, one arm: two fused contributions beat any single 1/(60+rank).
        assert set(names(result, ids)[:2]) == {"br", "mixed"}
    base_order = [name for name in names(off, ids) if name in {"br", "mixed"}]
    boost_ranks = {
        name: row["ranking"]["jurisdiction_boost"]["rank"]
        for name, row in zip(names(result, ids), result["results"], strict=True)
        if name in {"br", "mixed"}
    }
    assert sorted(boost_ranks, key=boost_ranks.get) == base_order
    assert sorted(boost_ranks.values()) == [1, 2]
    for row in result["results"]:
        boost = row["ranking"].get("jurisdiction_boost")
        assert bool(boost) == bool({"BR"} & set(row["jurisdiction"] or ()))
        if boost:
            assert boost["rrf_contribution"] == 1 / (60 + boost["rank"]) <= 1 / 61
        labels = [label for label in row["ranking"] if label != "jurisdiction_boost"]
        expected = sum(row["ranking"][label]["rrf_contribution"] for label in labels)
        assert row["score"] == pytest.approx(expected + (boost["rrf_contribution"] if boost else 0))
    disclosure = result["jurisdiction"]
    assert (disclosure["boosted_candidates"], disclosure["boosted_results"]) == (2, 2)
    assert disclosure["excluded_passages"] == 0 and disclosure["conflicting_passages"] == 2


def test_api_validates_and_discloses_the_rule(research):
    from fastapi.testclient import TestClient

    from ofl.research.api import create_app

    db, _ = research
    seed(db)
    client = TestClient(create_app(db.settings))
    headers = {"X-OFL-Gateway-Token": "test-gateway", "X-OFL-Identity": "reader"}
    bad = client.get(
        "/api/v1/search", params={"q": "inflação", "jurisdiction": "only"}, headers=headers
    )
    assert bad.status_code == 422
    plain = client.get("/api/v1/search", params={"q": "inflação Brasil"}, headers=headers).json()
    off = client.get(
        "/api/v1/search", params={"q": "inflação Brasil", "jurisdiction": "off"}, headers=headers
    ).json()
    assert plain == off and "jurisdiction" not in plain
    applied = client.get(
        "/api/v1/search",
        params={"q": "inflação OR Brasil", "jurisdiction": "exclude", "source": "news"},
        headers=headers,
    ).json()
    assert applied["jurisdiction"]["excluded_passages"] == 2


def test_rule_latency_on_fixture(research):
    """Observed overhead of the disclosure count on the fixture; reported, loosely bounded."""
    from ofl.research.search import search

    db, _ = research
    seed(db)

    def timed(rule):
        started = time.perf_counter()
        for _ in range(20):
            search(db, "inflação OR Brasil", mode="hybrid", vector=[1, 0, 0], jurisdiction=rule)
        return (time.perf_counter() - started) / 20

    timings = {rule: timed(rule) for rule in ("off", "boost", "exclude")}
    print("jurisdiction latency (s/query):", json.dumps(timings))
    assert max(timings.values()) < 1.0


def test_heldout_cases_validate_for_pooled_collection():
    from ofl.research.retrieval_review import MAX_CASES, _prepare, ranking_key

    path = ROOT / "docs/product/retrieval-cases-heldout.json"
    if not path.exists():
        pytest.skip("held-out file is not part of the mutation copy")
    cases = json.loads(path.read_text())
    assert len(_prepare(cases)) == len(cases["cases"]) == MAX_CASES
    assert [ranking_key("hybrid", v) for v in ("off", "boost", "exclude")] == [
        "hybrid",
        "hybrid+boost",
        "hybrid+exclude",
    ]


def test_collect_pools_variants_into_one_blinded_worksheet(research, monkeypatch):
    from ofl.research import retrieval_review
    from ofl.research.retrieval_review import assess, collect, worksheet

    db, _ = research
    ids = seed(db)
    monkeypatch.setattr(retrieval_review, "embed", lambda settings, texts: [[1, 0, 0]])
    specification = {
        "version": 1,
        "cases": [
            {"id": "H", "query": "inflação OR Brasil", "filters": {"source": "news"}},
            {"id": "N", "query": "inflação", "filters": {}},
        ],
    }
    with pytest.raises(ValueError, match="off, boost or exclude"):
        collect(db, specification, ["off", "only"])
    pool = json.loads(
        json.dumps(collect(db, specification, ["off", "boost", "exclude", "boost"]), default=str)
    )
    keys = [f"{m}{s}" for s in ("", "+boost", "+exclude") for m in ("keyword", "vector", "hybrid")]
    assert all(list(case["rankings"]) == keys for case in pool["cases"])
    assert pool["jurisdiction"]["variants"] == ["off", "boost", "exclude"]
    rankings = pool["cases"][0]["rankings"]
    assert ids["us"] in rankings["hybrid"]["item_ids"]
    assert ids["us"] not in rankings["hybrid+exclude"]["item_ids"]
    assert rankings["hybrid+exclude"]["jurisdiction"]["excluded_passages"] == 2
    assert "jurisdiction" not in rankings["hybrid"]
    sheet = worksheet(pool)
    text = json.dumps(sheet)
    assert "boost" not in text and "exclude" not in text and "jurisdiction" not in text
    sheet["reviewer"] = "fixture"
    report = assess(pool, sheet)
    assert list(report["summary"]) == keys
    plain = json.loads(json.dumps(collect(db, specification), default=str))
    assert "jurisdiction" not in plain and list(plain["cases"][0]["rankings"]) == keys[:3]
    broken = json.loads(json.dumps(pool))
    del broken["cases"][1]["rankings"]["hybrid+boost"]
    with pytest.raises(ValueError, match="same rankings"):
        assess(broken, worksheet(broken) | {"reviewer": "fixture"})


def test_audit_sample_is_stratified_seeded_and_private(research, tmp_path):
    db, _ = research
    seed(db)
    audit = audit_tool()
    first = audit.sample(db, 10, "seed-a", "news")
    assert first["population"] == {"BR": 1, "US": 1, "both": 1, "none": 2, "other": 1}
    assert [row["stratum"] for row in first["rows"]] == [
        "BR",
        "US",
        "both",
        "none",
        "none",
        "other",
    ]
    assert audit.sample(db, 10, "seed-a", "news")["rows"] == first["rows"]
    assert audit.sample(db, 5, "seed-a", "manager_letter")["rows"] == []
    assert all(
        row["review"]["tags"] == dict.fromkeys(row["jurisdiction"], "not_assessed")
        for row in first["rows"]
    )
    sheet = json.loads(json.dumps(first, default=str))
    assert audit.assess(sheet)["strata"]["none"]["rows"] == 2
    path = tmp_path / "sample.json"
    audit.private_write(path, sheet)
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        audit.private_write(path, sheet)
