"""Curated vocabulary expansion: lexicon, query plan, keyword/vector arms and disclosure."""

import asyncio
import json
import re
from types import SimpleNamespace

import pytest

pytest.importorskip("psycopg")

from ofl.research import lexical
from ofl.research import search as searching
from ofl.research import vocabulary as lexicon
from ofl.research.vocabulary import (
    LEXICON,
    VERSION,
    around,
    check_setting,
    disclosure,
    expanded_text,
    matches,
    plan,
    quoted_spans,
    term,
)

from tests.test_research import research  # noqa: E402,F401

CONCEPTS = [entry[0] for entry in LEXICON]


def concept(name):
    return CONCEPTS.index(name)


def applied(query):
    return [(c["concept"], c["matched"]) for c in plan("on", query)["concepts"]]


def skipped(query):
    return [(c["concept"], c["matched"], c["reason"]) for c in plan("on", query)["skipped"]]


def test_lexicon_entries_are_unique_documented_and_parse():
    assert VERSION == "vocab-v1.1"
    assert len(set(CONCEPTS)) == len(CONCEPTS) >= 25
    for name, pattern, expansions, home, rationale in LEXICON:
        assert re.fullmatch(r"[a-z]+(?:-[a-z]+)*", name)
        re.compile(pattern)
        assert expansions and len(set(expansions)) == len(expansions)
        assert home in {None, "BR"}
        assert len(rationale) > 40
        for expansion in expansions:
            text, phrase = term(expansion)
            assert text.startswith('"') and phrase and '"' not in phrase


def test_around_reads_the_modifier_on_either_side():
    assert around("a", "b") == "(?:a) (?:b)|(?:b) (?:a)"
    pattern = re.compile(around(lexicon.INFLATION, lexicon.BR))
    assert pattern.fullmatch("inflacao no brasil") and pattern.fullmatch("brazilian inflation")
    assert not pattern.fullmatch("inflacao")


def test_settings_are_off_or_on():
    assert check_setting("off") == "off" and check_setting("on") == "on"
    for bad in ("yes", "ON", "", None):
        with pytest.raises(ValueError, match="^vocabulary must be off or on$"):
            check_setting(bad)
    with pytest.raises(ValueError, match="off or on"):
        plan("boost", "inflação")


@pytest.mark.parametrize(
    "query,spans",
    [
        ("inflação", []),
        ('a "b c" d', [(2, 7)]),
        ('a "b', [(2, 4)]),
        ('"x""y"', [(0, 3), (3, 6)]),
        ('"', [(0, 1)]),
        ('a "b" "c', [(2, 5), (6, 8)]),
    ],
)
def test_quoted_spans_cover_quotes_and_run_unclosed_to_the_end(query, spans):
    assert quoted_spans(query) == spans


def test_longest_then_earliest_match_wins_without_overlap():
    assert matches("inflação Brasil") == [(0, 15, concept("inflation-br"))]
    assert matches("INFLAÇÃO E PIB") == [
        (0, 8, concept("inflation")),
        (11, 14, concept("gdp-br")),
    ]
    assert matches("pib pib") == [(0, 3, concept("gdp-br")), (4, 7, concept("gdp-br"))]
    # Decomposed accents: spans index the text as given, not the folded text.
    decomposed = "Inflação no Brasil hoje"
    assert matches(decomposed) == [(0, 20, concept("inflation-br"))]
    assert decomposed[:20] == "Inflação no Brasil"
    # Whole words only; a hyphen joins words.
    for query in ("inflacionário", "pré-inflação", "inflação-Brasil", "PIBs", "xpib"):
        assert [m for m in matches(query) if m[0] == 0] == []
    assert matches("inflação-Brasil") == []


@pytest.mark.parametrize(
    "first,second,query,expected",
    [
        # A longer later match beats a shorter earlier one it overlaps.
        ("a b", "b c d", "a b c d", [(2, 7, 1)]),
        # Touching matches both stay, in either order of selection.
        ("ab", "cde", "abcde", [(0, 2, 0), (2, 5, 1)]),
        ("abc", "de", "abcde", [(0, 3, 0), (3, 5, 1)]),
    ],
)
def test_overlap_resolution(monkeypatch, first, second, query, expected):
    patterns = [
        ("first", re.compile(first), ("A",), None),
        ("second", re.compile(second), ("B",), None),
    ]
    monkeypatch.setattr(lexicon, "PATTERNS", patterns)
    assert matches(query) == expected


def test_equal_spans_keep_the_earlier_lexicon_entry(monkeypatch):
    patterns = [
        ("first", re.compile("pib"), ("A",), None),
        ("second", re.compile("pib"), ("B",), None),
    ]
    monkeypatch.setattr(lexicon, "PATTERNS", patterns)
    assert matches("pib") == [(0, 3, 0)]
    monkeypatch.setattr(lexicon, "PATTERNS", patterns[::-1])
    assert matches("pib") == [(0, 3, 0)]


@pytest.mark.parametrize(
    "query,expected",
    [
        ("PIB do Brasil", False),
        ("PIB brasileiro", False),
        ("PIB", False),
        ("PIB da China", True),
        ("PIB dos EUA", True),
        ("PIB do Japão", True),
        ("PIB da zona do euro", True),
        ("PIB -China", False),
    ],
)
def test_other_jurisdictions_are_any_but_brazil(query, expected):
    assert lexicon._others(query) is expected


def test_off_never_reads_the_query():
    assert plan("off", "inflação Brasil") == {
        "setting": "off",
        "lexicon": VERSION,
        "concepts": [],
        "skipped": [],
    }


def test_plan_expands_concepts_and_keeps_the_remainder():
    result = plan("on", "inflação Brasil  em 2026")
    assert result == {
        "setting": "on",
        "lexicon": VERSION,
        "concepts": [
            {
                "concept": "inflation-br",
                "matched": "inflação Brasil",
                "expansions": list(LEXICON[concept("inflation-br")][2]),
            }
        ],
        "skipped": [],
        "remainder": "em 2026",
    }
    two = plan("on", "inflação e PIB no trimestre")
    assert [c["concept"] for c in two["concepts"]] == ["inflation", "gdp-br"]
    assert two["remainder"] == "e no trimestre"
    assert plan("on", "PIB")["remainder"] == ""
    none = plan("on", "quarks supercondutores")
    assert none["concepts"] == none["skipped"] == []
    assert none["remainder"] == "quarks supercondutores"


@pytest.mark.parametrize(
    "query,expected_applied,expected_skipped",
    [
        # A hyphen joins words, so `-inflação` never matches a concept at all.
        ("juros -inflação", [], []),
        ("juros - inflação", [], [("inflation", "inflação", "negated_or_quoted")]),
        ('"inflação Brasil" hoje', [], [("inflation-br", "inflação Brasil", "negated_or_quoted")]),
        ('PIB "inflação', [("gdp-br", "PIB")], [("inflation", "inflação", "negated_or_quoted")]),
        ("inflação OR juros", [], [("inflation", "inflação", "or_operator")]),
        ("PIB or juros", [], [("gdp-br", "PIB", "or_operator")]),
        ('PIB "a or b"', [("gdp-br", "PIB")], []),
        ("PIB orçamento", [("gdp-br", "PIB")], []),
        # Quotes separate words: `or` touching a quote is still the operator.
        ('PIB or"x"', [], [("gdp-br", "PIB", "or_operator")]),
        ('"x"or PIB', [], [("gdp-br", "PIB", "or_operator")]),
        ('"a"PIB', [("gdp-br", "PIB")], []),
        ('PIB"a"', [("gdp-br", "PIB")], []),
        ('"inflação" PIB', [("gdp-br", "PIB")], [("inflation", "inflação", "negated_or_quoted")]),
        (
            "inflação or PIB",
            [],
            [("inflation", "inflação", "or_operator"), ("gdp-br", "PIB", "or_operator")],
        ),
        (
            "PIB da China e inflação",
            [("inflation", "inflação")],
            [("gdp-br", "PIB", "other_jurisdiction")],
        ),
        ("PIB da China", [], [("gdp-br", "PIB", "other_jurisdiction")]),
        ("bolsa de Xangai", [], [("stocks", "bolsa", "other_jurisdiction")]),
        ("inflação na China", [("inflation", "inflação")], []),
        (
            "PIB e desemprego nos EUA",
            [("unemployment-us", "desemprego nos EUA")],
            [("gdp-br", "PIB", "other_jurisdiction")],
        ),
        ("Bolsa Família", [], []),
        ("bolsa", [("stocks", "bolsa")], []),
        ("juros básicos", [("policy-rate-basic", "juros básicos")], []),
        ("taxa básica de juros", [("policy-rate-basic", "taxa básica de juros")], []),
        ("juros no Brasil", [("policy-rate-br", "juros no Brasil")], []),
        # Brazil-default rates yield to any other named or mentioned jurisdiction.
        (
            "juros básicos nos EUA",
            [],
            [("policy-rate-basic", "juros básicos", "other_jurisdiction")],
        ),
        (
            "juros básicos na zona do euro",
            [],
            [("policy-rate-basic", "juros básicos", "other_jurisdiction")],
        ),
        (
            "juros básicos na China",
            [],
            [("policy-rate-basic", "juros básicos", "other_jurisdiction")],
        ),
        ("taxa básica do Fed", [], [("policy-rate-basic", "taxa básica", "other_jurisdiction")]),
        ("Brazilian inflation", [("inflation-br", "Brazilian inflation")], []),
        ("inflação Estados Unidos", [("inflation-us", "inflação Estados Unidos")], []),
    ],
)
def test_plan_skips_negated_quoted_or_and_other_jurisdiction_concepts(
    query, expected_applied, expected_skipped
):
    assert applied(query) == expected_applied
    assert skipped(query) == expected_skipped


def test_home_check_runs_once_per_query(monkeypatch):
    calls = []
    monkeypatch.setattr(lexicon, "_others", lambda query: calls.append(query) or False)
    assert len(applied("PIB e desemprego")) == 2
    assert calls == ["PIB e desemprego"]


def test_term_quotes_plain_expansions_and_keeps_websearch_syntax():
    assert term("IPCA-15") == ('"IPCA-15"', "IPCA-15")
    assert term('"IPCA" -"Tesouro IPCA"') == ('"IPCA" -"Tesouro IPCA"', "IPCA")


def test_expanded_text_appends_the_named_phrases():
    rule = {"concepts": [{"expansions": ["A", '"B" -"C"']}, {"expansions": ["D"]}]}
    assert expanded_text(rule, "inflação  Brasil") == "inflação Brasil (A, B, D)"


def test_disclosure_lists_applied_and_skipped_expansions():
    assert disclosure(plan("off", "PIB"), ["keyword"]) is None
    rule = plan("on", 'PIB "inflação"')
    assert disclosure(rule, ("keyword", "vector")) == {
        "setting": "on",
        "lexicon": VERSION,
        "applied": True,
        "arms": ["keyword", "vector"],
        "recognized": rule["concepts"],
        "expanded": ["gdp-br"],
        "skipped": rule["skipped"],
        "qualification": lexicon.QUALIFICATION,
    }
    assert rule["skipped"] and rule["concepts"]
    empty = disclosure(plan("on", "quarks"), ["keyword"])
    assert empty["applied"] is False and empty["arms"] == [] and empty["expanded"] == []
    # A recognized concept that no arm searched is not applied.
    idle = disclosure(rule, [])
    assert idle["applied"] is False and idle["arms"] == [] and idle["expanded"] == []
    assert idle["recognized"] == rule["concepts"]


def test_arms_follow_mode_and_request():
    on, none = plan("on", "PIB"), plan("on", "quarks")
    both = ("vector", "keyword")
    assert searching._arms(none, "hybrid", both) == []
    assert searching._arms(plan("off", "PIB"), "hybrid", both) == []
    assert searching._arms(on, "hybrid", both) == ["keyword", "vector"]
    assert searching._arms(on, "keyword", both) == ["keyword"]
    assert searching._arms(on, "vector", both) == ["vector"]
    assert searching._arms(on, "vector", ["keyword"]) == []
    assert searching._arms(on, "hybrid") == list(lexicon.ARMS)
    assert searching._arms(on, "hybrid", ()) == []
    with pytest.raises(ValueError, match="^vocabulary arms must be keyword and/or vector$"):
        searching._arms(on, "hybrid", ["keyword", "jurisdiction"])


class Parser:
    """Stands in for PostgreSQL's websearch parse: each piece's text, tagged."""

    settings = None

    def __init__(self):
        self.calls = []

    def rows(self, query, params=()):
        self.calls.append((query, params))
        return [{"parsed": [f"'{piece}'" if piece else "" for piece in params[0]]}]


def test_keyword_terms_without_expansion_are_the_typed_operands():
    fake = Parser()
    terms = searching._keyword_terms(fake, "PIB", plan("off", "PIB"), [])
    assert terms == {
        "strict": "'PIB'",
        "ranked": "(( 'PIB' ))",
        "matched": "(( 'PIB' ))",
        "operands": ["'PIB'"],
        "expanded": None,
    }
    assert fake.calls == [(searching.PARSE, (["PIB"],))]
    unarmed = searching._keyword_terms(Parser(), "PIB", plan("on", "PIB"), ["vector"])
    assert unarmed == terms


class Repeated(Parser):
    """`selic Selic juros jurídico`: repeated and same-stem words."""

    def rows(self, query, params=()):
        return [{"parsed": ["'selic' & 'selic' & 'jur' & !'eua' & 'jur'"]}]


def test_keyword_terms_count_each_distinct_operand_once():
    terms = searching._keyword_terms(Repeated(), "selic Selic juros -EUA juro", {}, [])
    assert terms["operands"] == ["'selic'", "'jur'"]
    assert terms["ranked"] == "(( 'selic' ) | ( 'jur' )) & !'eua'"
    assert terms["strict"] == "'selic' & 'selic' & 'jur' & !'eua' & 'jur'"
    assert lexical.min_should_match(len(terms["operands"]), "ceil60") == 1


def test_keyword_terms_without_positive_operands_keep_the_parsed_query():
    class Negated(Parser):
        def rows(self, query, params=()):
            return [{"parsed": ["!'eua'"]}]

    terms = searching._keyword_terms(Negated(), "-EUA", plan("off", "-EUA"), [])
    assert terms == {
        "strict": "!'eua'",
        "ranked": "!'eua'",
        "matched": "!'eua'",
        "operands": [],
        "expanded": None,
    }


def test_keyword_terms_make_each_concept_one_operand():
    fake = Parser()
    rule = {
        "concepts": [
            {"matched": "inflação", "expansions": ["A b", '"C" -"D"']},
            {"matched": "PIB", "expansions": ["E"]},
        ],
        "remainder": "em 2026",
    }
    terms = searching._keyword_terms(fake, "inflação PIB em 2026", rule, ["keyword"])
    pieces = ["inflação PIB em 2026", "em 2026", "inflação", '"A b"', '"C" -"D"', "PIB", '"E"']
    assert fake.calls == [(searching.PARSE, (pieces,))]
    groups = ["( 'inflação' ) | ( '\"A b\"' ) | ( '\"C\" -\"D\"' )", "( 'PIB' ) | ( '\"E\"' )"]
    assert terms["operands"] == ["'em 2026'", *groups]
    assert terms["expanded"] == lexical.disjunction(["'em 2026'", *groups], [])
    assert terms["matched"] == terms["expanded"]
    assert terms["strict"] == "'inflação PIB em 2026'"
    assert terms["ranked"] == "(( 'inflação PIB em 2026' ))"


def test_concept_groups_drop_repeated_alternatives_and_groups():
    class Twice(Parser):
        def rows(self, query, params=()):
            return [{"parsed": ["'a'", "", "'a'", "'b'", "'a'", "'a'", "'b'", "'a'"]}]

    rule = {
        "concepts": [
            {"matched": "a", "expansions": ["b", "c"]},
            {"matched": "a", "expansions": ["b", "c"]},
        ],
        "remainder": "",
    }
    terms = searching._keyword_terms(Twice(), "a a", rule, ["keyword"])
    assert terms["operands"] == ["( 'a' ) | ( 'b' )"]
    assert terms["matched"] == "(( ( 'a' ) | ( 'b' ) ))"


def test_keyword_terms_skip_empty_alternatives_and_fall_back_without_operands():
    class Empty(Parser):
        def rows(self, query, params=()):
            return [{"parsed": ["", "", "", ""]}]

    rule = {"concepts": [{"matched": "de", "expansions": ["a"]}], "remainder": "o"}
    terms = searching._keyword_terms(Empty(), "de o", rule, ["keyword"])
    assert terms["operands"] == [] and terms["expanded"] is None
    assert terms["matched"] == terms["ranked"] == ""


def test_keyword_query_orders_coverage_first():
    terms = {
        "strict": "s",
        "ranked": "r",
        "matched": "m",
        "operands": ["a", "b", "c"],
        "expanded": None,
    }
    sql, head, minimum = searching._keyword_query("p.id", "p.active", "PIB", terms)
    assert sql == searching.KEYWORD.format(
        projection="p.id",
        where="p.active",
        heading=searching.HEADING.format(config=lexical.CONFIG),
        literal=lexical.LITERAL,
    )
    assert head == ("s", "r", "m", ["a", "b", "c"], 0.0, "r", "PIB") and minimum == 1
    tail = searching._keyword_tail(minimum, 100)
    assert sql.count("%s") == len(head) + len(tail) and tail == (1, 100, 1, 100)
    assert "DISTINCT ON (h.hit_item)" in sql and "LIMIT %s" in sql
    assert searching._keyword_query("p.id", "p.active", "PIB", terms, "ceil60")[2] == 2
    expanded = terms | {"expanded": "x"}
    head = searching._keyword_query("p.id", "p.active", "PIB", expanded)[1]
    assert head[4:6] == (lexicon.EXPANSION_WEIGHT, "x")


def test_query_vectors_embed_only_what_is_missing(monkeypatch):
    calls = []

    rule, settings = plan("on", "PIB"), SimpleNamespace()

    def fake(given, texts):
        assert given is settings
        calls.append(list(texts))
        return [[float(len(calls)), float(i)] for i in range(len(texts))]

    monkeypatch.setattr(searching, "embed", fake)
    expanded = expanded_text(rule, "PIB")
    assert searching._query_vectors(settings, "PIB", rule, ["keyword"]) == [[1.0, 0.0]]
    assert calls == [["PIB"]]
    assert searching._query_vectors(settings, "PIB", rule, ["vector"]) == [[2.0, 0.0], [2.0, 1.0]]
    assert calls[-1] == ["PIB", expanded]
    given = searching._query_vectors(settings, "PIB", rule, ["vector"], vector=[9.0])
    assert given == [[9.0], [3.0, 0.0]] and calls[-1] == [expanded]
    both = searching._query_vectors(settings, "PIB", rule, ["vector"], vector=[9.0], expanded=[8.0])
    assert both == [[9.0], [8.0]] and len(calls) == 3
    alone = searching._query_vectors(settings, "PIB", rule, ["keyword"], vector=[7.0])
    assert alone == [[7.0]] and len(calls) == 3


@pytest.mark.parametrize("approximate", [False, True])
def test_vector_rows_rank_by_distance_within_the_scope(approximate):
    calls = []
    settings = SimpleNamespace(
        embedding_dimensions=3,
        approximate_search=approximate,
        embedding_model="m",
        embedding_revision="r",
    )

    class Fake:
        def rows(self, query, params=(), *, ann=False):
            calls.append((query, params, ann))
            return ["row"]

    Fake.settings = settings
    scope = ("p.id", "p.active AND i.source=%s", ["news"])
    assert searching._vector_rows(Fake(), scope, [1, 0, 0], 7) == ["row"]
    query, params, ann = calls[0]
    literal = "[1.0,0.0,0.0]"
    assert params == (literal, "news", "m", "r", literal, 7) and ann is approximate
    expression = "e.embedding::vector(3)" if approximate else "e.embedding"
    assert f"SELECT p.id,1-({expression} <=> %s::vector) AS score" in query
    assert f"ORDER BY {expression} <=> %s::vector,p.id LIMIT %s" in query
    assert "WHERE p.active AND i.source=%s AND " + searching.SEMANTIC_CONTENT in query
    assert "e.model=%s AND e.revision=%s" in query
    with pytest.raises(ValueError, match="3 finite dimensions"):
        searching._vector_rows(Fake(), scope, [1, 0], 7)


CORPUS = {
    "plain": ("Inflação no Brasil acelera", "A inflação brasileira sobe de novo."),
    "index": ("IPCA de setembro sobe 0,5%", "O índice oficial acelera."),
    "bond": ("Tesouro IPCA mais rende", "Título público paga taxa."),
    "foreign": ("Preços nos EUA", "A inflação americana sobe."),
    "grant": ("Bolsa Família paga benefício", "Calendário de pagamentos."),
}
VECTORS = {
    "plain": "[1,0,0]",
    "index": "[0,1,0]",
    "bond": "[0,0,1]",
    "foreign": "[0.9,0.1,0]",
    "grant": "[0.5,0.5,0.5]",
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
    return ids


def names(result, ids):
    reverse = {v: k for k, v in ids.items()}
    return [reverse[row["item_id"]] for row in result["results"]]


@pytest.mark.parametrize("mode", ["keyword", "vector", "hybrid"])
def test_off_is_identical_and_undisclosed(research, mode):
    from ofl.research.search import search

    db, _ = research
    seed(db)
    base = search(db, "inflação Brasil", mode=mode, vector=[1, 0, 0])
    assert search(db, "inflação Brasil", mode=mode, vector=[1, 0, 0], vocabulary="off") == base
    assert "vocabulary" not in base
    with pytest.raises(ValueError, match="off or on"):
        search(db, "inflação", mode=mode, vector=[1, 0, 0], vocabulary="boost")


def test_keyword_expansion_adds_expansion_only_matches_at_a_discount(research):
    from ofl.research.search import search

    db, _ = research
    ids = seed(db)
    off = search(db, "inflação Brasil", mode="keyword")
    assert "index" not in names(off, ids) and names(off, ids)[0] == "plain"
    on = search(db, "inflação Brasil", mode="keyword", vocabulary="on")
    # The `Tesouro IPCA` bond is excluded from the IPCA expansion.
    assert names(on, ids)[0] == "plain" and "index" in names(on, ids)
    assert "bond" not in names(on, ids)
    score = {name: row["score"] for name, row in zip(names(on, ids), on["results"], strict=True)}
    # One concept operand: every match covers it; the typed words rank first.
    assert all(1 <= value < 2 for value in score.values())
    assert score["plain"] > score["index"]
    assert on["vocabulary"] == disclosure(plan("on", "inflação Brasil"), ["keyword"])


def test_negated_concepts_are_never_expanded_in_search(research):
    from ofl.research.search import search

    db, _ = research
    ids = seed(db)
    for query in ("sobe -inflação", 'sobe "inflação Brasil"'):
        off = search(db, query, mode="keyword", limit=50)
        on = search(db, query, mode="keyword", limit=50, vocabulary="on")
        assert names(on, ids) == names(off, ids)
        assert on["vocabulary"]["applied"] is False and on["vocabulary"]["arms"] == []


@pytest.mark.parametrize("mode", ["vector", "hybrid"])
def test_vector_expansion_is_one_bounded_extra_fused_list(research, mode):
    from ofl.research.search import search

    db, _ = research
    ids = seed(db)
    arms = ("keyword", "vector")
    kwargs = {"mode": mode, "vector": [1, 0, 0], "limit": 50, "vocabulary": "on"}
    plain = search(db, "inflação Brasil", **kwargs)
    assert all("vocabulary_vector" not in row["ranking"] for row in plain["results"])
    assert plain["vocabulary"]["arms"] == ([] if mode == "vector" else ["keyword"])
    assert plain["vocabulary"]["applied"] is (mode != "vector")
    assert plain["vocabulary"]["expanded"] == ([] if mode == "vector" else ["inflation-br"])
    assert plain["vocabulary"]["recognized"][0]["concept"] == "inflation-br"
    result = search(
        db, "inflação Brasil", **kwargs, vocabulary_arms=arms, expanded_vector=[0, 1, 0]
    )
    assert result["vocabulary"]["arms"] == (
        ["vector"] if mode == "vector" else ["keyword", "vector"]
    )
    first = result["results"][0]
    vector = {
        name: row["ranking"]["vector"]["rank"]
        for name, row in zip(names(result, ids), result["results"], strict=True)
    }
    assert vector["plain"] == 1 and vector["index"] > 1
    extra = {
        name: row["ranking"]["vocabulary_vector"]
        for name, row in zip(names(result, ids), result["results"], strict=True)
    }
    assert extra["index"]["rank"] == 1 and extra["index"]["raw_score"] == pytest.approx(1)
    for row in result["results"]:
        contribution = row["ranking"]["vocabulary_vector"]["rrf_contribution"]
        assert contribution == 1 / (60 + row["ranking"]["vocabulary_vector"]["rank"]) <= 1 / 61
        assert row["score"] == pytest.approx(
            sum(entry["rrf_contribution"] for entry in row["ranking"].values())
        )
    assert first["score"] >= result["results"][1]["score"]


def test_vector_expansion_embeds_the_expanded_query(research, monkeypatch):
    from ofl.research.search import search

    db, _ = research
    seed(db)
    seen = []

    def fake(settings, texts):
        seen.append(list(texts))
        return [[1, 0, 0], [0, 1, 0]][: len(texts)]

    monkeypatch.setattr(searching, "embed", fake)
    result = search(
        db, "inflação Brasil", mode="vector", vocabulary="on", vocabulary_arms=["vector"]
    )
    expanded = expanded_text(plan("on", "inflação Brasil"), "inflação Brasil")
    assert seen == [["inflação Brasil", expanded]]
    assert result["results"][0]["ranking"]["vocabulary_vector"]


def test_unindexed_scope_never_reports_the_vector_arm(research):
    from ofl.research.search import search

    db, _ = research
    seed(db)
    with db.connect() as con:
        con.execute("DELETE FROM embeddings")
    result = search(
        db,
        "inflação Brasil",
        mode="hybrid",
        vocabulary="on",
        vocabulary_arms=("keyword", "vector"),
    )
    assert result["vocabulary"]["arms"] == ["keyword"]
    assert all(set(row["ranking"]) == {"keyword"} for row in result["results"])


def test_api_validates_and_discloses_the_vocabulary(research):
    from fastapi.testclient import TestClient

    from ofl.research.api import create_app

    db, _ = research
    seed(db)
    client = TestClient(create_app(db.settings))
    headers = {"X-OFL-Gateway-Token": "test-gateway", "X-OFL-Identity": "reader"}
    bad = client.get(
        "/api/v1/search", params={"q": "inflação", "vocabulary": "yes"}, headers=headers
    )
    assert bad.status_code == 422
    plain = client.get("/api/v1/search", params={"q": "inflação Brasil"}, headers=headers).json()
    off = client.get(
        "/api/v1/search", params={"q": "inflação Brasil", "vocabulary": "off"}, headers=headers
    ).json()
    assert plain == off and "vocabulary" not in plain
    on = client.get(
        "/api/v1/search", params={"q": "inflação Brasil", "vocabulary": "on"}, headers=headers
    ).json()
    assert on["vocabulary"]["lexicon"] == VERSION and len(on["results"]) == 2
    assert on["vocabulary"]["recognized"][0]["concept"] == "inflation-br"
    assert on["vocabulary"]["expanded"] == ["inflation-br"]


def test_mcp_validates_and_forwards_the_vocabulary():
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
        await server.call_tool("search_archive", {"q": "PIB"})
        await server.call_tool("search_archive", {"q": "PIB", "vocabulary": "on"})
        with pytest.raises(Exception):
            await server.call_tool("search_archive", {"q": "PIB", "vocabulary": "yes"})

    asyncio.run(exercise())
    assert "vocabulary" not in seen[0] and seen[1]["vocabulary"] == "on"
    assert len(seen) == 2


def test_collect_pools_vocabulary_variants_into_one_blinded_worksheet(research, monkeypatch):
    from ofl.research import retrieval_review
    from ofl.research.retrieval_review import assess, collect, worksheet

    db, _ = research
    ids = seed(db)
    embedded = []

    def fake(settings, texts):
        embedded.append(list(texts))
        return [[0, 1, 0] if "(" in texts[0] else [1, 0, 0]]

    monkeypatch.setattr(retrieval_review, "embed", fake)
    specification = {
        "version": 1,
        "cases": [
            {"id": "V", "query": "inflação Brasil", "filters": {"source": "news"}},
            {"id": "N", "query": "quarks", "filters": {}},
        ],
    }
    with pytest.raises(ValueError, match="off, boost or exclude"):
        collect(db, specification, ["off", "vocab-x"])
    pool = json.loads(
        json.dumps(collect(db, specification, ["off", "vocab-k", "vocab-kv"]), default=str)
    )
    keys = [
        f"{m}{s}" for s in ("", "+vocab-k", "+vocab-kv") for m in ("keyword", "vector", "hybrid")
    ]
    assert all(list(case["rankings"]) == keys for case in pool["cases"])
    assert pool["vocabulary"] == {"variants": ["vocab-k", "vocab-kv"], "lexicon": VERSION}
    assert "jurisdiction" not in pool
    # The expanded query is embedded once, only for the case with a concept.
    assert embedded == [
        ["inflação Brasil"],
        [expanded_text(plan("on", "inflação Brasil"), "inflação Brasil")],
        ["quarks"],
    ]
    rankings = pool["cases"][0]["rankings"]
    assert ids["index"] not in rankings["keyword"]["item_ids"]
    assert ids["index"] in rankings["keyword+vocab-k"]["item_ids"]
    assert rankings["hybrid+vocab-kv"]["vocabulary"]["arms"] == ["keyword", "vector"]
    assert rankings["hybrid+vocab-k"]["vocabulary"]["arms"] == ["keyword"]
    assert "vocabulary" not in rankings["hybrid"]
    text = json.dumps(worksheet(pool))
    assert "vocab" not in text and "IPCA-15" not in text
    report = assess(pool, worksheet(pool) | {"reviewer": "fixture"})
    assert list(report["summary"]) == keys
    mixed = collect(db, specification, ["off", "exclude", "vocab-k"])
    assert mixed["jurisdiction"]["variants"] == ["off", "exclude"]
    assert mixed["vocabulary"]["variants"] == ["vocab-k"]


def add(db, name, title, body, vector="[0,0,1]"):
    from ofl.research.archive import index_text, upsert_item

    with db.connect() as con:
        item = upsert_item(con, "news", {"source_id": name, "title": title}, name)
        index_text(
            con, item_id=item, revision=name, text=f"{title}\n{body}", method="collector_news"
        )
        passage = con.execute(
            "SELECT id FROM passages WHERE item_id=%s AND active", (item,)
        ).fetchone()["id"]
        con.execute(
            "INSERT INTO embeddings VALUES (%s,%s,%s,%s::vector)",
            (passage, db.settings.embedding_model, db.settings.embedding_revision, vector),
        )
    return item


@pytest.mark.parametrize("rule", ["off", "exclude"])
@pytest.mark.parametrize(
    "query", ["juros básicos nos EUA", "juros básicos na zona do euro", "juros básicos na China"]
)
def test_brazil_default_rates_never_expand_for_other_jurisdictions(research, query, rule):
    from ofl.research.search import search

    db, _ = research
    seed(db)
    selic = add(db, "selic", "Copom mantém a Selic", "A Selic segue em 15%.")
    add(db, "rates", "Juros básicos nos EUA, zona do euro e China", "Bancos centrais decidem.")
    off = search(db, query, mode="keyword", limit=50, jurisdiction=rule)
    on = search(db, query, mode="keyword", limit=50, jurisdiction=rule, vocabulary="on")
    assert [r["id"] for r in on["results"]] == [r["id"] for r in off["results"]]
    assert [r["score"] for r in on["results"]] == [r["score"] for r in off["results"]]
    assert selic not in {r["item_id"] for r in on["results"]}
    assert on["vocabulary"]["applied"] is False and on["vocabulary"]["recognized"] == []
    assert [s["reason"] for s in on["vocabulary"]["skipped"]] == ["other_jurisdiction"]
    # Control: unqualified, the Brazil default applies.
    plain = search(db, "juros básicos", mode="keyword", limit=50, vocabulary="on")
    assert selic in {r["item_id"] for r in plain["results"]}


# Frozen from main before vocabulary support: off must keep this SQL and its ranking.
MAIN_KEYWORD = """WITH query AS (
                SELECT websearch_to_tsquery('portuguese',%s) AS pt,
                    websearch_to_tsquery('simple',%s) AS literal
                ) SELECT {projection},ts_rank_cd(p.search,
                    CASE WHEN p.method='metadata' AND strpos(query.literal::text,'!')=0
                    THEN query.pt || query.literal ELSE query.pt END) AS score
                FROM passages p JOIN items i ON i.id=p.item_id CROSS JOIN query
                WHERE {where} AND (p.search @@ query.pt OR
                    (p.method='metadata' AND strpos(query.literal::text,'!')=0
                    AND p.search @@ query.literal))
                ORDER BY score DESC,p.id LIMIT %s"""
MAIN_VECTOR = """SELECT {projection},1-({expression} <=> %s::vector) AS score
            FROM passages p JOIN items i ON i.id=p.item_id JOIN embeddings e ON e.passage_id=p.id
            WHERE {where} AND {semantic} AND e.model=%s AND e.revision=%s
            ORDER BY {expression} <=> %s::vector,p.id LIMIT %s"""


def test_off_keeps_every_all_terms_match_and_main_vector_ranking(research):
    from ofl.research.search import PROJECTION, SEMANTIC_CONTENT, scope_conditions, search

    db, _ = research
    seed(db)
    where, params = scope_conditions(source="news")
    for query in ("inflação", "inflação Brasil", "inflação -EUA", '"inflação americana"'):
        everything = db.rows(
            f"""SELECT DISTINCT p.item_id FROM passages p JOIN items i ON i.id=p.item_id
            WHERE {where} AND p.search @@ websearch_to_tsquery(%s,research_search_text(%s))""",
            (*params, lexical.CONFIG, query),
        )
        result = search(db, query, mode="keyword", source="news", limit=50)
        found = [r["item_id"] for r in result["results"]]
        assert {r["item_id"] for r in everything} <= set(found)
        # Matches holding every term come first: they cover every operand.
        assert set(found[: len(everything)]) == {r["item_id"] for r in everything}
    vector_sql = MAIN_VECTOR.format(
        projection=PROJECTION, expression="e.embedding", where=where, semantic=SEMANTIC_CONTENT
    )
    literal = "[1.0,0.0,0.0]"
    space = (db.settings.embedding_model, db.settings.embedding_revision)
    main_vector = db.rows(vector_sql, (literal, *params, *space, literal, 100))
    ours = searching._vector_rows(db, (PROJECTION, where, params), [1, 0, 0], 100)
    assert [(r["id"], r["score"]) for r in ours] == [(r["id"], r["score"]) for r in main_vector]


def test_failed_expanded_embedding_fails_only_rankings_that_need_it(research, monkeypatch):
    from ofl.research import retrieval_review
    from ofl.research.retrieval_review import collect

    db, _ = research
    seed(db)

    def fake(settings, texts):
        if "(" in texts[0]:
            raise RuntimeError("expanded embedding refused")
        return [[1, 0, 0]]

    monkeypatch.setattr(retrieval_review, "embed", fake)
    specification = {"version": 1, "cases": [{"id": "V", "query": "inflação Brasil"}]}
    pool = collect(db, specification, ["off", "vocab-k", "vocab-kv"])
    status = {key: r["status"] for key, r in pool["cases"][0]["rankings"].items()}
    assert status == {
        "keyword": "ok",
        "vector": "ok",
        "hybrid": "ok",
        "keyword+vocab-k": "ok",
        "vector+vocab-k": "ok",
        "hybrid+vocab-k": "ok",
        "keyword+vocab-kv": "ok",
        "vector+vocab-kv": "error",
        "hybrid+vocab-kv": "error",
    }

    def broken(settings, texts):
        raise RuntimeError("model unavailable")

    monkeypatch.setattr(retrieval_review, "embed", broken)
    pool = collect(db, specification, ["off", "vocab-kv"])
    status = {key: r["status"] for key, r in pool["cases"][0]["rankings"].items()}
    assert {k for k, v in status.items() if v == "ok"} == {"keyword", "keyword+vocab-kv"}
