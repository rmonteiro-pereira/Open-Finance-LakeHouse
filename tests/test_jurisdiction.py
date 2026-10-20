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


def strong(title=0, body=0, related=0, unclassified=0):
    return {"title": title, "body": body, "related": related, "unclassified": unclassified}


@pytest.mark.parametrize(
    "text,expected",
    [
        ("O IPCA de março subiu e a Selic caiu.", ["BR"]),
        ("A Selic ficou estável após o Copom.", ["BR"]),
        ("Juros nos Estados Unidos sobem; o Fed reage.", ["US"]),
        ("O Fed manteve os juros; o FOMC se reúne.", ["US"]),
        ("Ações caem em Wall Street e na Nasdaq.", ["US"]),
        ("Inflação na zona do euro e no Banco Central Europeu.", ["EA"]),
        ("O BCE decidiu; o Eurostat publicou.", ["EA"]),
        ("A China cresce e o PBoC corta juros.", ["CN"]),
        ("Inflação no Brasil e no IPCA; nos EUA e no Fed.", ["BR", "US"]),
        ("Inflação de serviços desacelera.", []),
    ],
)
def test_two_strong_body_mentions_tag_passages(text, expected):
    assert codes(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "O IPCA de março subiu.",
        "Juros nos Estados Unidos sobem.",
        "Brasileiros pagam mais e brasileiras também.",
        "Preço de R$100 e R$ 200 no varejo.",
        "Ações norte-americanas e americanas caem.",
        "A atriz americana e a equipe americana venceram.",
        "Exportações chinesas para Pequim e Xangai.",
        "O yuan se valorizou contra o renminbi.",
        "O real se desvalorizou frente ao dólar; juros reais altos.",
        "O CPI subiu 0,3%; Treasuries e payroll movem o mercado.",
        "O Banco Central elevou os juros e o Tesouro vendeu títulos.",
        "O euro caiu na Europa.",
        "Ativos de US$ 5 bilhões e US$ 3 milhões.",
        "Torneio Shanghai Masters e evento em Washington.",
        "Mercados latino-americanos e sul-americanos.",
        "A fed-funds curve.",
        "Uma notícia escrita em português sobre inflação e juros.",
        "fed e bce em minúsculas não são siglas.",
        "Indochina e chinelos.",
    ],
)
def test_single_strong_or_weak_only_evidence_never_tags(text):
    assert codes(text) == []


@pytest.mark.parametrize(
    "text",
    [
        "Acidente na Rodovia Washington Luís. Trânsito na Washington Luís segue lento.",
        "Ações das Lojas Americanas caem. A Americanas renegocia dívidas.",
        "Produção de jornalismo da CNN Brasil. Programa da TV Brasil e da Agência Brasil.",
        "Final da Copa do Brasil. GP do Brasil.",
        "Liga de futebol americano; a moeda americana sobe.",
        "Wall Street Journal e Washington Post publicaram.",
        "Ações da China Mobile e da Air China.",
    ],
)
def test_reviewed_entities_with_country_words_never_count(text):
    result = tag(text)
    assert result["jurisdiction"] == []
    assert result["evidence"]["entities"] >= 2
    assert all(m["strength"] in {"entity", "weak"} for m in result["evidence"]["matches"])


def test_weak_markers_are_recorded_per_tier_but_do_not_support_a_tag():
    result = tag("O real caiu e o CPI subiu.")
    assert result["jurisdiction"] == []
    assert result["evidence"]["counts"] == {
        "BR": {"strong": strong(), "weak": strong(body=1)},
        "US": {"strong": strong(), "weak": strong(body=1)},
    }
    assert result["evidence"]["decisions"] == {
        "BR": "no_strong_evidence",
        "US": "no_strong_evidence",
    }
    assert [(m["term"], m["strength"], m["where"]) for m in result["evidence"]["matches"]] == [
        ("real", "weak", "body"),
        ("CPI", "weak", "body"),
    ]
    assert tag("O Brasil cresce.")["evidence"]["decisions"] == {"BR": "single_body"}


def test_dominance_rule_requires_two_strong_mentions_for_a_minor_jurisdiction():
    major = "O Fed e o FOMC; nos EUA, Wall Street"
    assert codes(f"{major}.", "Brasil") == ["US"]
    result = tag(f"{major}.", "Brasil")
    assert result["evidence"]["decisions"] == {"BR": "dominated", "US": "body"}
    assert codes("O Fed e o FOMC.", "Brasil") == ["BR", "US"]
    assert codes(f"{major}. Brasil e IPCA.") == ["BR", "US"]
    assert codes(f"{major}. Brasil.") == ["US"]
    assert codes("EUA e Fed.", "Brasil e China") == ["BR", "CN", "US"]
    counts = {
        "BR": {"strong": strong(title=1), "weak": strong()},
        "US": {"strong": strong(body=3), "weak": strong()},
    }
    assert places.decide(counts) == {"BR": "dominated", "US": "body"}
    counts["US"]["strong"]["body"] = 2
    assert places.decide(counts) == {"BR": "title", "US": "body"}
    counts["BR"]["strong"] = strong(title=1, related=5, unclassified=5)
    counts["US"]["strong"] = strong(body=3)
    assert places.decide(counts)["BR"] == "dominated"


def test_entities_and_strong_longer_matches_mask_overlapping_markers():
    result = tag("O Banco Central Europeu e a zona do euro; o Tesouro Nacional e o Ibovespa.")
    assert result["jurisdiction"] == ["BR", "EA"]
    terms = [(m["term"], m["code"], m["strength"]) for m in result["evidence"]["matches"]]
    assert terms == [
        ("Banco Central Europeu", "EA", "strong"),
        ("zona do euro", "EA", "strong"),
        ("Tesouro Nacional", "BR", "strong"),
        ("Ibovespa", "BR", "strong"),
    ]
    assert [(s, e, c, t) for s, e, c, t in find("Banco Central do Brasil")] == [
        (0, 13, "BR", "weak"),
        (17, 23, "BR", "strong"),
    ]
    assert find("Lojas Americanas") == [(0, 16, None, "entity")]


def test_related_link_and_navigation_text_never_tags():
    text = (
        "Inflação de serviços desacelera no Brasil e o IPCA sobe. Leia também: Inflação nos "
        "EUA dispara e Fed reage. Tags: China, zona do euro"
    )
    result = tag(text)
    assert result["jurisdiction"] == ["BR"]
    counts = result["evidence"]["counts"]
    assert counts["US"]["strong"] == strong(related=2)
    assert counts["CN"]["strong"]["related"] == 1 and counts["EA"]["strong"]["related"] == 1
    only_related = tag("Mercados. LEIA MAIS: Estados Unidos e China em guerra comercial.")
    assert only_related["jurisdiction"] == []
    # A marker covers its own sentence only, however little text follows.
    assert codes("Veja também: EUA e Fed. O IPCA subiu e a Selic caiu.") == ["BR"]
    assert codes("Leia também o relatório. China e PBoC elevam juros.", "Fed mantém juros") == [
        "CN",
        "US",
    ]


def test_abbreviations_do_not_end_a_related_headline():
    assert tag("Leia também: Juros nos U.S. e China sobem.")["jurisdiction"] == []
    assert tag("Leia também: Juros nos U.S. e China e PBoC sobem.")["jurisdiction"] == []
    assert tag("Leia também: O Sr. Silva fala da China e do PBoC.")["jurisdiction"] == []
    assert codes("Veja mais: E.U.A. e China; PBoC age. China e PBoC.") == ["CN"]
    assert places.sentence_end("Nos U.S. e E.U.A. o Dr. fala. Fim.", 0) == 29
    assert places.sentence_end("Nos U.S.", 0) is None
    assert places.sentence_end("Ele saiu… Fim.", 0) == 9
    assert places.sentence_end('A taxa (U.S.) e "Sr." caem. Fim.', 0) == 27
    quoted = 'A taxa (U.S. e "Sr. caem. Fim.'
    assert places.sentence_end(quoted, 0) == quoted.index("caem.") + 5
    # Only the 40 characters before a period are read: a longer token is not initials.
    assert places.sentence_end("x." * 21 + " Fim.", 0) == 42
    assert places.sentence_end("O plano X. Fim.", 0) == 15


def test_unmarked_closing_text_keeps_body_provenance():
    body = " ".join(["O mercado subiu hoje."] * 20)
    for text in (f"{body} China e PBoC elevam juros", f"{body} Brasil"):
        assert related_spans(text) == []
    assert codes(f"{body} China e PBoC elevam juros", "Fed mantém juros") == ["CN", "US"]
    two = f"{body} Leia também: um. Veja mais: dois. China e PBoC."
    first, second = two.index("Leia"), two.index("Veja")
    assert related_spans(two) == [(first, second - 1), (second, two.index(" China"))]
    assert codes(two) == ["CN"]


def test_state_banks_are_weak_national_evidence_not_masked():
    result = tag("Lucro do Banco do Brasil sobe; Banco do Brasil paga dividendos.")
    assert result["jurisdiction"] == []
    assert result["evidence"]["counts"]["BR"]["weak"]["body"] == 2
    assert result["evidence"]["entities"] == 0
    china = tag("Bank of China e China Construction Bank emprestam.")
    assert china["evidence"]["counts"]["CN"]["weak"]["body"] == 2
    assert codes("Banco do Brasil e IPCA; o Brasil cresce.") == ["BR"]
    assert detect("Banco do Brasil") == [] and detect("Bank of China") == []


def test_european_demonyms_never_name_the_euro_area_in_queries():
    for query in ("inflação europeia", "bancos europeus", "economia da Europa", "euro forte"):
        assert detect(query) == []
    for query in ("inflação na zona do euro", "decisão do BCE", "HICP do Eurostat"):
        assert detect(query) == ["EA"]
    assert detect("inflação brasileira") == ["BR"] and detect("juros americanos") == ["US"]
    assert (
        tag("Bancos europeus e a economia europeia.")["evidence"]["counts"]["EA"]["weak"]["body"]
        == 2
    )


def test_decision_boundaries():
    counts = {
        "BR": {"strong": strong(title=1, body=1), "weak": strong()},
        "US": {"strong": strong(body=6), "weak": strong()},
    }
    assert places.decide(counts) == {"BR": "title", "US": "body"}


def test_entities_never_name_a_jurisdiction_in_queries():
    assert detect("Americanas") == []
    assert detect("Lojas Americanas e Brasil") == ["BR"]
    assert detect("futebol americano") == []
    assert detect('-"x"Brasil') == ["BR"]
    assert tag("Lojas Americanas")["evidence"]["entities"] == 1
    assert tag("Lojas Americanas e CNN Brasil")["evidence"]["entities"] == 2


def test_related_region_ends_at_sentence_or_word_bound():
    filler = " ".join(f"w{i}" for i in range(200))
    assert related_spans(f"Leia também: EUA sobem. Brasil cai. {filler}") == [(0, 23)]
    words = " ".join(f"w{i}" for i in range(40))
    text = f"Saiba mais {words} Brasil e IPCA. {filler}"
    ((start, end),) = related_spans(text)
    assert start == 0 and text[end - 3 : end] == "w29"
    assert codes(text) == ["BR"]
    assert related_spans("Sem marcador. EUA.") == []
    assert related_spans("Releia também os EUA.") == []
    words29 = " ".join(f"w{i}" for i in range(29))
    assert related_spans(f"Saiba mais {words29}") == [(0, len(f"Saiba mais {words29}"))]
    words30 = " ".join(f"w{i}" for i in range(30))
    assert related_spans(f"Saiba mais {words30}   ") == [(0, len(f"Saiba mais {words30}"))]


def test_feed_footers_and_marked_link_lists_are_related():
    body = " ".join(["O mercado subiu hoje."] * 20)
    footer = f"{body} The post Brasil e IPCA sobem appeared first on Portal Brasil."
    assert codes(footer) == []
    assert tag(footer)["evidence"]["counts"]["BR"]["strong"]["related"] == 3
    # An unpunctuated list after a marker is one block, up to 30 words.
    listing = f"{body} Leia também Brasil e IPCA sobem China e PBoC cortam juros"
    assert codes(listing) == []
    assert codes(f"{body} Brasil e IPCA preocupam.") == ["BR"]
    assert codes("Brasil e IPCA preocupam") == ["BR"]


def test_overlaps_prefer_entities_strong_then_longer_then_earlier(monkeypatch):
    import re

    def patterns(*entries):
        return [(code, tier, tier, re.compile(p)) for code, tier, p in entries]

    monkeypatch.setattr(
        places, "PATTERNS", patterns(("US", "weak", "abcdef"), ("BR", "strong", "cd"))
    )
    assert find("abcdef") == [(2, 4, "BR", "strong")]
    monkeypatch.setattr(
        places, "PATTERNS", patterns(("BR", "strong", "abcdef"), (None, "entity", "cd"))
    )
    assert find("abcdef") == [(2, 4, None, "entity")]
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
    monkeypatch.setattr(
        places, "PATTERNS", patterns(("BR", "strong", "ab"), ("US", "strong", "cde"))
    )
    assert find("abcde") == [(0, 2, "BR", "strong"), (2, 5, "US", "strong")]


def test_title_is_counted_once_and_inherited_by_every_passage():
    title = "Inflação  nos EUA"
    first = tag("Inflação nos EUA surpreende os analistas.", title)
    assert first["jurisdiction"] == ["US"]
    assert first["evidence"]["counts"]["US"] == {"strong": strong(title=1), "weak": strong()}
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
    assert tag("O IPCA sobe.", "Selic")["evidence"]["counts"]["BR"]["strong"]["body"] == 1
    assert tag("Brasil cresce. Brasil", "Brasil")["jurisdiction"] == ["BR"]
    assert tag("Brasil cresce. Brasil", "Brasil")["evidence"]["counts"]["BR"]["strong"] == (
        strong(title=1, body=1)
    )
    assert tag("Texto sem título", None)["jurisdiction"] == []
    assert codes("Atriz americana premiada.", "Atriz americana") == []


def test_match_list_is_bounded_but_counts_are_complete():
    result = tag("Brasil " * 60)
    assert len(result["evidence"]["matches"]) == places.MAX_MATCHES
    assert result["evidence"]["counts"]["BR"]["strong"]["body"] == 60


# Each negated jurisdiction term is negated by PostgreSQL's websearch parser too.
NEGATIONS = [
    ("inflação -Estados Unidos", []),
    ("juros -Banco Central Europeu", []),
    ("juros -zona do euro e China", ["CN"]),
    ('inflação -"Estados Unidos"', []),
    ('-"zona do euro" inflação', []),
    ('-"Estados Unidos', []),
    ('"Estados Unidos" -Brasil', ["US"]),
    ("inflação - Brasil", []),
    ("inflação OR -China", []),
    ("juros --EUA", []),
    ("juros -(EUA)", []),
    ('juros "-EUA"', ["US"]),
    ('juros"-EUA"', ["US"]),
    ('"inflação -EUA" Brasil', ["BR", "US"]),
    ("inflação -", []),
]


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
    ]
    + NEGATIONS,
)
def test_query_detection_fires_only_on_explicit_strong_names(query, expected):
    assert detect(query) == expected


def test_negations_match_postgresql_websearch_parser(research):
    db, _ = research
    for query, expected in NEGATIONS:
        parsed = db.rows("SELECT websearch_to_tsquery('simple',%s)::text AS q", (query,))[0]["q"]
        for name, code in (
            ("estados", "US"),
            ("eua", "US"),
            ("zona", "EA"),
            ("brasil", "BR"),
            ("china", "CN"),
        ):
            if f"'{name}'" in parsed:
                negated = f"!'{name}'" in parsed or f"!( '{name}'" in parsed
                assert negated == (code not in expected), (query, parsed)


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
        # boost is API-only.
        with pytest.raises(Exception):
            await server.call_tool("search_archive", {"q": "inflação", "jurisdiction": "boost"})

    asyncio.run(exercise())
    assert "jurisdiction" not in seen[0] and seen[1]["jurisdiction"] == "exclude"
    assert len(seen) == 2


# PostgreSQL contracts (disposable schema per test; see tests/test_research.py).
from tests.test_research import research  # noqa: E402,F401

CORPUS = {
    "br": ("IPCA sobe e inflação no Brasil acelera", "A inflação de alimentos pressiona o índice."),
    "mixed": (
        "Inflação nos EUA surpreende",
        "A inflação assusta; o Brasil acompanha e o IPCA sobe.",
    ),
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
    done = {"tagger": places.VERSION, "pending": 1, "tagged": 1, "partial_context": 0}
    assert backfill(db, dry_run=True) == done | {"tagged": 0}
    seen = []
    assert backfill(db, batch=1, progress=seen.append) == done
    assert seen == [done]
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
    assert rows[ids["br"]]["jurisdiction_evidence"]["counts"]["BR"]["strong"]["title"] == 2
    assert {r["jurisdiction_tagger"] for r in rows.values()} == {places.VERSION}
    assert db.rows("SELECT passage_id,embedding::text FROM embeddings ORDER BY 1") == embeddings
    with db.connect() as con:
        con.execute("UPDATE items SET title='Outro título' WHERE id=%s", (ids["br"],))
    assert backfill(db, dry_run=True)["pending"] == 1


def long_document():
    """A related-link region opened at word 305 runs past the 310-word chunk boundary."""
    words = [f"palavra{i}" for i in range(700)]
    words[300:305] = ["no", "Brasil", "e", "IPCA", "sobem."]
    words[305:307] = ["Leia", "também:"]
    words[320] = "EUA"
    words[400:404] = ["IPCA", "e", "Selic", "sobem."]
    return " ".join(words)


def test_fallback_regions_use_the_passage_text_and_unclassify_its_head():
    body = " ".join(["O mercado subiu hoje."] * 20)
    text = f"Leia também: EUA. {body} Brasil e IPCA"
    (region,) = places.passage_regions([1], [text])
    assert region["related"] == [(0, text.index(" O mercado"))]
    assert tag(text, **region)["jurisdiction"] == ["BR"]


def test_span_helpers_and_explicit_regions():
    assert places._word_spans(["ab", "c"]) == [(0, 2), (3, 4)]
    assert places._merge([(0, 3), (4, 7), (9, 10)]) == [(0, 7), (9, 10)]
    assert tag("x")["evidence"]["context"] == "passage"
    assert tag("x", context="document")["evidence"]["context"] == "document"

    def where(**regions):
        return [m["where"] for m in tag("T Fed", "T", **regions)["evidence"]["matches"]]

    assert where(related=[(2, 5)]) == ["related"]
    assert where(related=[(2, 4)]) == ["body"]
    assert where(related=[(3, 5)]) == ["body"]
    assert where(related=[], unclassified=[(0, 2)]) == ["body"]
    assert where(related=[], unclassified=[(4, 5)]) == ["unclassified"]
    assert where(related=[], unclassified=[(5, 6)]) == ["body"]
    assert where(related=[], unclassified=[(2, 3)]) == ["unclassified"]
    rest = " ".join(["O mercado subiu."] * 20)
    titled = tag(
        f"Inflação no Brasil Leia também: EUA dispara. O Fed age. {rest}", "Inflação no Brasil"
    )
    assert titled["evidence"]["counts"]["US"]["strong"]["related"] == 1
    assert titled["evidence"]["counts"]["US"]["strong"]["body"] == 1
    with pytest.raises(ValueError):
        places.passage_regions([0, 1], ["só um"])


@pytest.mark.parametrize(
    "query,spans",
    [
        ("a --EUA", [(4, 7)]),
        ('-"a b" "c"', [(1, 6)]),
        ('-"" x', [(1, 3)]),
        ('-"ab"x', [(1, 5)]),
        ('-ab"c"', [(1, 3)]),
        ('x"-EUA"', []),
        ("a - b", [(4, 5)]),
        ("Brasil-EUA", []),
    ],
)
def test_negated_spans_follow_websearch_terms(query, spans):
    assert places.negated_spans(query) == spans


def test_related_region_crossing_a_chunk_boundary_never_tags():
    from ofl.research.archive import chunks

    passages = list(chunks(long_document()))
    assert len(passages) == 3 and passages[1].split()[10] == "EUA"
    regions = places.passage_regions(range(3), passages)
    assert [r["context"] for r in regions] == ["document"] * 3
    assert places.document(passages) == long_document().split()
    first, second = (places._word_spans(p.split()) for p in passages[:2])
    # Region: "Leia também:" (words 305-306) and the next 30 words, through word 336.
    assert regions[0]["related"] == [(first[305][0], first[336][1])]
    assert regions[1]["related"] == [(0, second[26][1])]
    assert regions[2]["related"] == []
    tags = [tag(text, "", **region) for text, region in zip(passages, regions, strict=True)]
    assert [t["jurisdiction"] for t in tags] == [["BR"], ["BR"], []]
    assert tags[1]["evidence"]["counts"]["US"]["strong"]["related"] == 1
    assert tags[0]["evidence"]["counts"]["US"]["strong"]["related"] == 1
    # Passage text alone would misread the boundary; the old behaviour is the regression.
    assert (
        tag(passages[1], related=related_spans(passages[1]))["evidence"]["counts"]["US"]["strong"][
            "body"
        ]
        == 1
    )
    partial = places.passage_regions([1, 2], passages[1:])
    assert [r["context"] for r in partial] == ["passage", "passage"]
    assert partial[0]["unclassified"] == [(0, second[29][1])]
    alone = tag(passages[1], "", **partial[0])
    assert alone["jurisdiction"] == ["BR"]
    assert alone["evidence"]["counts"]["US"]["strong"]["unclassified"] == 1
    broken = [passages[0], passages[1].replace("palavra310", "outra", 1), passages[2]]
    assert places.passage_regions(range(3), broken)[1]["context"] == "passage"
    assert places.document(passages[:1]) == passages[0].split()
    assert places.passage_regions([0], [passages[0]])[0]["unclassified"] == []


def test_ingestion_and_backfill_share_document_context(research):
    from ofl.research.archive import index_text, upsert_item
    from ofl.research.jurisdiction import backfill

    db, _ = research
    with db.connect() as con:
        item = upsert_item(con, "news", {"source_id": "long", "title": "Serviços"}, "long")
        index_text(con, item_id=item, revision="r", text=long_document(), method="collector_news")
    query = """SELECT ordinal,jurisdiction,jurisdiction_evidence FROM passages
        WHERE item_id=%s AND method='collector_news' ORDER BY ordinal"""
    ingested = db.rows(query, (item,))
    assert [r["jurisdiction"] for r in ingested] == [["BR"], ["BR"], []]
    assert {r["jurisdiction_evidence"]["context"] for r in ingested} == {"document"}
    with db.connect() as con:
        con.execute("UPDATE passages SET jurisdiction=NULL,jurisdiction_tagger=NULL")
    assert backfill(db, batch=1)["partial_context"] == 0
    assert db.rows(query, (item,)) == ingested
    with db.connect() as con:
        con.execute(
            """UPDATE passages SET jurisdiction=NULL,jurisdiction_tagger=NULL WHERE item_id=%s""",
            (item,),
        )
        con.execute(
            "DELETE FROM passages WHERE item_id=%s AND ordinal=0 AND method='collector_news'",
            (item,),
        )
    assert backfill(db)["partial_context"] == 2
    rows = db.rows(query, (item,))
    assert [r["jurisdiction"] for r in rows] == [["BR"], []]
    assert rows[0]["jurisdiction_evidence"]["counts"]["US"]["strong"]["unclassified"] == 1


def test_migration_fails_fast_when_a_table_is_locked(research):
    import psycopg

    db, _ = research
    with psycopg.connect(db.settings.database_url) as holder:
        holder.execute("LOCK TABLE passages IN ACCESS SHARE MODE")
        started = time.perf_counter()
        with pytest.raises(RuntimeError, match="could not lock a research table within 5 s"):
            db.migrate()
        assert time.perf_counter() - started < 15
    db.migrate()


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
        "ignored": [],
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


@pytest.mark.parametrize("query", ["inflação -Estados Unidos", 'inflação -"Estados Unidos"'])
def test_partially_negated_names_never_apply_a_rule(research, query):
    from ofl.research.search import search

    db, _ = research
    seed(db)
    off = search(db, query, mode="keyword", limit=50)
    for rule in ("boost", "exclude"):
        result = search(db, query, mode="keyword", limit=50, jurisdiction=rule)
        assert result["jurisdiction"]["applied"] is False
        assert [r["id"] for r in result["results"]] == [r["id"] for r in off["results"]]


def test_closing_text_about_the_named_jurisdiction_survives_exclusion(research):
    from ofl.research.archive import index_text, upsert_item
    from ofl.research.search import search

    db, _ = research
    body = " ".join(["O comitê avaliou a inflação."] * 15)
    documents = {
        "unmarked": f"Fed mantém juros {body} China e PBoC elevam juros",
        "after_link": f"Fed mantém juros {body} Leia também o relatório. China e PBoC elevam juros.",
        "us_only": f"Fed mantém juros {body} O FOMC volta a se reunir.",
    }
    ids = {}
    with db.connect() as con:
        for name, text in documents.items():
            ids[name] = upsert_item(
                con, "news", {"source_id": name, "title": "Fed mantém juros"}, name
            )
            con.execute("UPDATE passages SET active=false WHERE item_id=%s", (ids[name],))
            index_text(con, item_id=ids[name], revision=name, text=text, method="collector_news")
    tags = {
        r["item_id"]: r["jurisdiction"]
        for r in db.rows("SELECT item_id,jurisdiction FROM passages WHERE active")
    }
    assert tags[ids["unmarked"]] == tags[ids["after_link"]] == ["CN", "US"]
    assert tags[ids["us_only"]] == ["US"]
    # CN is tagged but ungated: a query naming China is searched as off.
    off = search(db, "juros OR China", mode="keyword", limit=50)
    result = search(db, "juros OR China", mode="keyword", limit=50, jurisdiction="exclude")
    assert result["jurisdiction"] | {"qualification": None} == {
        "rule": "exclude",
        "tagger": places.VERSION,
        "named": ["CN"],
        "applied": False,
        "ignored": ["CN"],
        "qualification": None,
    }
    assert [r["id"] for r in result["results"]] == [r["id"] for r in off["results"]]
    assert len(result["results"]) == len(documents)


@pytest.mark.parametrize("mode", ["keyword", "vector", "hybrid"])
@pytest.mark.parametrize("query", ["inflação OR China", "inflação OR Brasil OR China"])
def test_ungated_jurisdictions_search_exactly_as_off(research, mode, query):
    from ofl.research.search import search

    db, _ = research
    seed(db)
    vector = [1, 0, 0]
    off = search(db, query, mode=mode, vector=vector, limit=50)
    columns = {"jurisdiction", "jurisdiction_evidence", "jurisdiction_tagger"}
    for rule in ("boost", "exclude"):
        result = search(db, query, mode=mode, vector=vector, limit=50, jurisdiction=rule)
        disclosure = result.pop("jurisdiction")
        assert disclosure["applied"] is False and disclosure["ignored"] == ["CN"]
        assert disclosure["named"] == (["CN"] if "Brasil" not in query else ["BR", "CN"])
        assert not {"excluded_passages", "boosted_candidates"} & set(disclosure)
        stripped = [{k: v for k, v in r.items() if k not in columns} for r in result["results"]]
        assert result | {"results": stripped} == off
    gated = search(db, "inflação OR Brasil", mode=mode, vector=vector, limit=50)
    excluded = search(
        db, "inflação OR Brasil", mode=mode, vector=vector, limit=50, jurisdiction="exclude"
    )
    assert len(excluded["results"]) == len(gated["results"]) - 2


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


def test_latency_tool_is_read_only_and_reports_budget(research, monkeypatch):
    import psycopg

    from ofl.research import retrieval_latency
    from ofl.research.config import ResearchSettings
    from ofl.research.db import Database

    db, _ = research
    seed(db)
    monkeypatch.setattr(retrieval_latency, "embed", lambda settings, texts: [[1, 0, 0]])
    settings = ResearchSettings(
        **{
            **db.settings.model_dump(),
            "database_url": retrieval_latency.read_only(db.settings.database_url),
        }
    )
    reader = Database(settings)
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        reader.rows("UPDATE passages SET active=active")
    cases = [
        {"id": "named", "query": "inflação OR Brasil", "filters": {"source": "news"}},
        {"id": "neutral", "query": "inflação"},
    ]
    report = retrieval_latency.measure(reader, cases, repetitions=3, warmup=1)
    assert report["cases"] == ["named"] and report["repetitions"] == 3
    for mode in ("keyword", "hybrid"):
        rows = report["modes"][mode]
        assert rows["off"]["median_overhead"] == 0
        assert all(
            rows[r]["p95_ms"] >= rows[r]["median_ms"] > 0 for r in ("off", "boost", "exclude")
        )
        assert set(rows["per_case_median_overhead"]["named"]) == {"boost", "exclude"}
    assert report["budget_median_overhead"] == 0.2 and isinstance(report["within_budget"], bool)
    with pytest.raises(ValueError, match="names a jurisdiction"):
        retrieval_latency.measure(reader, cases[1:], repetitions=3)
    assert retrieval_latency.percentile([5, 1, 3, 2, 4], 0.95) == 5
    assert "default_transaction_read_only" in retrieval_latency.read_only(
        "postgresql:///x?options=-csearch_path%3Da"
    )


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
    taken = [first["rows"][0]["passage_id"]]
    again = audit.sample(db, 10, "seed-a", "news", exclude=taken)
    assert again["excluded_previous"] == 1 and len(again["rows"]) == len(first["rows"]) - 1
    assert taken[0] not in {row["passage_id"] for row in again["rows"]}
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
