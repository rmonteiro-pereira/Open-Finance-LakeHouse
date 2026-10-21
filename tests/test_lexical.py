"""Lexical search: unaccent configurations, locale-independent text, OR+coverage and rebuild."""

import re
from importlib.resources import files

import pytest

pytest.importorskip("psycopg")

from ofl.research import lexical
from ofl.research.lexical import (
    disjunction,
    document_params,
    min_should_match,
    operands,
    split,
)
from tests.test_research import research  # noqa: F401


@pytest.mark.parametrize(
    "parsed,expected",
    [
        ("", []),
        ("'a'", ["'a'"]),
        ("'a' & 'b'", ["'a'", "'b'"]),
        ("'a' & 'b' <-> 'c' | !'d'", ["'a'", "'b' <-> 'c'", "!'d'"]),
        ("'a' & !( 'b' <-> 'c' )", ["'a'", "!( 'b' <-> 'c' )"]),
        ("( 'a' & 'b' ) | 'c'", ["( 'a' & 'b' )", "'c'"]),
        ("'it''s' & 'x&y' | 'p|q'", ["'it''s'", "'x&y'", "'p|q'"]),
        ("'a' <2> 'b'", ["'a' <2> 'b'"]),
        ("'a'&'b'", ["'a'", "'b'"]),
        ("!( ( 'a' | 'b' ) <-> 'c' ) & 'd'", ["!( ( 'a' | 'b' ) <-> 'c' )", "'d'"]),
        ("'a''' & 'b'", ["'a'''", "'b'"]),
    ],
)
def test_operands_split_only_at_top_level_operators(parsed, expected):
    assert operands(parsed) == expected


def test_split_and_disjunction_keep_negations_as_and():
    assert split(["'a'", "!'b'", "'c'", "!( 'd' )"]) == (["'a'", "'c'"], ["!'b'", "!( 'd' )"])
    assert disjunction([], ["!'b'"]) is None
    assert disjunction(["'a'"], []) == "(( 'a' ))"
    assert disjunction(["'a'", "'b' <-> 'c'"], ["!'d'", "!'e'"]) == (
        "(( 'a' ) | ( 'b' <-> 'c' )) & !'d' & !'e'"
    )


@pytest.mark.parametrize(
    "count,mode,expected",
    [
        (0, "none", 0),
        (1, "none", 1),
        (5, "none", 1),
        (0, "ceil60", 0),
        (1, "ceil60", 1),
        (2, "ceil60", 1),
        (3, "ceil60", 2),
        (4, "ceil60", 3),
        (5, "ceil60", 3),
        (6, "ceil60", 4),
    ],
)
def test_min_should_match(count, mode, expected):
    assert min_should_match(count, mode) == expected


def test_min_should_match_defaults_and_validates(monkeypatch):
    assert lexical.MIN_SHOULD_MATCH in lexical.MODES
    monkeypatch.setattr(lexical, "MIN_SHOULD_MATCH", "ceil60")
    assert min_should_match(3) == 2
    monkeypatch.setattr(lexical, "MIN_SHOULD_MATCH", "none")
    assert min_should_match(3) == 1
    for bad in ("all", "", None):
        with pytest.raises(ValueError, match="^min-should-match must be none or ceil60$"):
            min_should_match(3, bad or "x")


def test_document_params_index_metadata_literally():
    assert document_params("PETR4 ação", "metadata") == ("PETR4 ação", "PETR4 ação")
    assert document_params("corpo", "ocr") == ("", "corpo")
    assert lexical.DOCUMENT.count("%s") == 2
    assert "research_search_text" in lexical.DOCUMENT and lexical.CONFIG in lexical.DOCUMENT


def test_schema_translates_exactly_the_documented_characters():
    schema = files("ofl.research").joinpath("schema.sql").read_text()
    source, target = re.search(r"translate\(value, '([^']*)', '([^']*)'\)", schema).groups()
    assert source == lexical.QUOTES + lexical.SPACES
    assert target == '"' * len(lexical.QUOTES) + " " * len(lexical.SPACES)


def item(db, name, title, body, method="collector_news"):
    from ofl.research.archive import index_text, upsert_item

    with db.connect() as con:
        identity = upsert_item(con, "news", {"source_id": name, "title": title}, name)
        index_text(con, item_id=identity, revision=name, text=body, method=method)
    return identity


def keyword(db, query, **options):
    from ofl.research.search import search

    return [r["item_id"] for r in search(db, query, mode="keyword", limit=50, **options)["results"]]


def test_accents_case_and_typographic_punctuation_do_not_matter(research):
    db, _ = research
    a = item(db, "a", "Inflação acelera", "A inflação de “serviços” sobe — Índice cheio.")
    b = item(db, "b", "Cambio", "O CÂMBIO e a politica monetaria do Banco Central.")
    for query, expected in (
        ("inflação", [a]),
        ("inflacao", [a]),
        ("INFLAÇÃO", [a]),
        ("serviços", [a]),
        ("servicos", [a]),
        ("indice", [a]),
        ("“serviços”", [a]),
        ("câmbio", [b]),
        ("política monetária", [b]),
    ):
        assert keyword(db, query) == expected, query
    vector = db.rows(
        "SELECT search::text AS v FROM passages WHERE item_id=%s AND method<>'metadata'", (a,)
    )[0]["v"]
    assert "“" not in vector and "—" not in vector and "'indic'" in vector
    assert db.rows("SELECT research_search_text('“a” — b…') AS t")[0]["t"] == '"a"   b '


def test_or_coverage_ranks_items_by_distinct_terms_then_rank(research):
    db, _ = research
    full = item(db, "full", "Nota", "A Selic e os juros americanos comparados.")
    # Two passages of one item hold the terms between them: item-level coverage.
    split_item = item(
        db, "split", "Nota", "A Selic sobe. " + "palavra " * 400 + "Juros americanos comparados."
    )
    assert len(db.rows("SELECT 1 FROM passages WHERE item_id=%s AND active", (split_item,))) > 2
    one = item(db, "one", "Bolsa", "Os juros sobem.")
    none = item(db, "none", "Clima", "Chuva forte no sul.")
    found = keyword(db, "Selic juros americanos comparados")
    assert found[:2] == [full, split_item] and found[2] == one and none not in found
    from ofl.research.search import search

    scores = {
        r["item_id"]: r["score"]
        for r in search(db, "Selic juros americanos comparados", mode="keyword")["results"]
    }
    assert 4 <= scores[full] < 5 and 4 <= scores[split_item] < 5 and 1 <= scores[one] < 2
    assert scores[full] > scores[split_item]


def test_negations_and_phrases_keep_their_meaning(research):
    db, _ = research
    us = item(db, "us", "EUA", "Inflação nos EUA sobe.")
    br = item(db, "br", "Brasil", "Inflação no Brasil sobe.")
    phrase = item(db, "phrase", "Juros", "Juros futuros sobem.")
    loose = item(db, "loose", "Futuros", "Os futuros e os juros.")
    assert keyword(db, "inflação -EUA") == [br]
    assert us not in keyword(db, "inflação sobe -EUA")
    assert keyword(db, '"juros futuros"') == [phrase]
    assert set(keyword(db, '"juros futuros" sobem')) == {phrase, us, br}
    assert keyword(db, '"juros futuros" sobem')[0] == phrase
    assert loose not in keyword(db, '"juros futuros"')


def test_min_should_match_ceil60_drops_low_coverage(research, monkeypatch):
    db, _ = research
    three = item(db, "three", "x", "Selic juros americanos.")
    one = item(db, "one", "y", "Selic apenas.")
    assert set(keyword(db, "Selic juros americanos")) == {three, one}
    monkeypatch.setattr(lexical, "MIN_SHOULD_MATCH", "ceil60")
    assert keyword(db, "Selic juros americanos") == [three]


def test_metadata_identifiers_still_match_literally(research):
    db, _ = research
    meta = item(db, "meta", "Fundo", "PETR4 fundos", method="metadata")
    assert keyword(db, "PETR4") == [meta]
    assert keyword(db, "fundos") == [meta]


def test_rebuild_rewrites_stale_vectors_resumably(research):
    import psycopg

    db, _ = research
    a = item(db, "a", "Inflação", "A inflação de “serviços” sobe.")
    b = item(db, "b", "Câmbio", "O câmbio oscila.")
    current = {
        r["id"]: r["v"] for r in db.rows("SELECT id,search::text AS v FROM passages ORDER BY id")
    }
    assert {r["v"] for r in db.rows("SELECT search_version AS v FROM passages")} == {
        lexical.VERSION
    }
    with db.connect() as con:
        con.execute(
            """UPDATE passages SET search=to_tsvector('portuguese',text),search_version=NULL"""
        )
    assert keyword(db, "inflacao") == []
    total = len(current)
    assert lexical.rebuild(db, dry_run=True) == {
        "lexical": lexical.VERSION,
        "pending": total,
        "rebuilt": 0,
    }
    steps = []
    with psycopg.connect(db.settings.database_url) as holder:
        locked = sorted(current)[0]
        holder.execute("SELECT 1 FROM passages WHERE id=%s FOR UPDATE", (locked,))
        partial = lexical.rebuild(db, batch=1, progress=steps.append)
    assert partial == {
        "lexical": lexical.VERSION,
        "pending": total,
        "rebuilt": total - 1,
        "remaining": 1,
    }
    assert steps == [
        {"lexical": lexical.VERSION, "pending": total, "rebuilt": n} for n in range(1, total)
    ]
    done = lexical.rebuild(db, batch=1)
    assert done == {"lexical": lexical.VERSION, "pending": 1, "rebuilt": 1, "remaining": 0}
    rebuilt = {
        r["id"]: r["v"] for r in db.rows("SELECT id,search::text AS v FROM passages ORDER BY id")
    }
    assert rebuilt == current
    assert keyword(db, "inflacao") == [a] and keyword(db, "cambio") == [b]
    assert lexical.rebuild(db) == {
        "lexical": lexical.VERSION,
        "pending": 0,
        "rebuilt": 0,
        "remaining": 0,
    }


def test_migration_is_idempotent(research):
    db, _ = research
    db.migrate()
    db.migrate()
    configs = {
        r["cfgname"]
        for r in db.rows(
            """SELECT c.cfgname FROM pg_ts_config c JOIN pg_namespace n ON n.oid=c.cfgnamespace
            WHERE n.nspname=current_schema()"""
        )
    }
    assert {"portuguese_unaccent", "simple_unaccent"} <= configs


def test_vectors_and_queries_do_not_depend_on_the_database_locale(research):
    """A C-locale database used to store `É` unfolded and `“ministro”` fused."""
    import os
    import uuid
    from urllib.parse import urlsplit, urlunsplit

    import psycopg
    from psycopg import sql

    from ofl.research.config import ResearchSettings
    from ofl.research.db import Database

    db, _ = research
    base = urlsplit(os.environ["OFL_RESEARCH_TEST_DATABASE_URL"])
    name = "research_c_" + uuid.uuid4().hex[:12]
    try:
        with psycopg.connect(urlunsplit(base), autocommit=True) as con:
            con.execute(
                sql.SQL("CREATE DATABASE {} LOCALE 'C' ENCODING 'UTF8' TEMPLATE template0").format(
                    sql.Identifier(name)
                )
            )
    except psycopg.Error as error:
        pytest.skip(f"cannot create a C-locale database: {error.sqlstate}")
    other = urlunsplit(base._replace(path="/" + name))
    text = "É o “ministro” — disse: Índice de preços… ÇÃO • «Brasil» ‘aspas’"
    query = "ÍNDICE “ministro” preço"
    try:
        with psycopg.connect(other, autocommit=True) as con:
            con.execute("CREATE EXTENSION IF NOT EXISTS vector")
        Database(ResearchSettings(**{**db.settings.model_dump(), "database_url": other})).migrate()
        results = []
        for url in (other, db.settings.database_url):
            with psycopg.connect(url) as con:
                ctype = con.execute(
                    "SELECT datctype FROM pg_database WHERE datname=current_database()"
                ).fetchone()[0]
                vector = con.execute(
                    f"SELECT ({lexical.DOCUMENT})::text", document_params(text, "ocr")
                ).fetchone()[0]
                parsed = con.execute(
                    "SELECT websearch_to_tsquery(%s,research_search_text(%s))::text",
                    (lexical.CONFIG, query),
                ).fetchone()[0]
            results.append((ctype, vector, parsed))
        assert results[0][0] == "C"
        assert results[0][1:] == results[1][1:]
        assert results[0][1] == (
            "'aspas':10B 'brasil':9B 'cao':8B 'diss':4B 'indic':5B 'ministr':3B 'prec':7B"
        )
        assert results[0][2] == "'indic' & 'ministr' & 'prec'"
    finally:
        with psycopg.connect(urlunsplit(base), autocommit=True) as con:
            con.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))


def test_keyword_fusion_variants():
    from ofl.research import search as searching

    rows = [{"id": "a", "covered": 3}, {"id": "b", "covered": 2}, {"id": "c", "covered": 1}]
    assert searching._keyword_fusion(rows, 3, "plain") is rows
    assert searching._keyword_fusion(rows, 0, "weighted") is rows
    assert searching._keyword_fusion([], 3, "floor") == []
    assert searching._keyword_fusion(rows, 3, "weighted") == [
        {"id": "a", "covered": 3, "fusion_weight": 1.0},
        {"id": "b", "covered": 2, "fusion_weight": 2 / 3},
        {"id": "c", "covered": 1, "fusion_weight": 1 / 3},
    ]
    assert searching._keyword_fusion(rows, 3, "floor") == rows[:2]
    assert searching._keyword_fusion(rows[1:], 3, "floor") == rows[1:]
    assert searching._keyword_fusion(rows, 3) == searching._keyword_fusion(
        rows, 3, searching.FUSION
    )
    with pytest.raises(ValueError, match="^keyword fusion must be plain, weighted or floor$"):
        searching._keyword_fusion(rows, 3, "max")


def test_fusion_weights_scale_each_share():
    from ofl.research import search as searching

    keyword = [{"id": "a", "score": 1.0, "fusion_weight": 0.5}, {"id": "b", "score": 0.5}]
    vector = [{"id": "b", "score": 0.9}, {"id": "a", "score": 0.8}]
    fused = {row["id"]: row["score"] for row in searching.fuse([keyword, vector])}
    assert fused == {"a": pytest.approx(0.5 / 61 + 1 / 62), "b": pytest.approx(1 / 62 + 1 / 61)}
    shares = searching._contributions(["keyword", "vector"], [keyword, vector])
    assert shares["a"]["keyword"] == {"rank": 1, "raw_score": 1.0, "rrf_contribution": 0.5 / 61}
    assert shares["b"]["keyword"]["rrf_contribution"] == 1 / 62
    alone = searching._contributions(["keyword"], [keyword])
    assert alone["a"]["keyword"]["rrf_contribution"] is None


def test_negation_only_excludes_and_keeps_the_rest(research):
    db, _ = research
    us = item(db, "us", "EUA", "Inflação nos EUA sobe.")
    br = item(db, "br", "Brasil", "Inflação no Brasil sobe.")
    found = keyword(db, "-EUA")
    assert br in found and us not in found


def test_empty_unbalanced_and_long_queries(research):
    db, _ = research
    phrase = item(db, "phrase", "Juros", "Juros futuros sobem.")
    item(db, "loose", "Futuros", "Os futuros e os juros.")
    assert keyword(db, "de o a") == []
    assert keyword(db, '"juros futuros') == keyword(db, '"juros futuros"') == [phrase]
    words = " ".join(f"p{n}" for n in range(200)) + " futuros"
    assert len(words) <= 1000
    assert phrase in keyword(db, words)


MAIN_KEYWORD = """SELECT DISTINCT p.item_id FROM passages p WHERE p.active
    AND p.search @@ websearch_to_tsquery('portuguese',%s)"""


def test_rebuild_rolls_back_to_legacy_vectors_for_earlier_code(research):
    db, _ = research
    a = item(db, "a", "Nota", "A inflação e o câmbio sobem.")
    b = item(db, "b", "Nota", "A inflação cai.")
    # Rebuilt vectors are unaccented: code before lexical-v1 would miss accented words.
    assert {r["item_id"] for r in db.rows(MAIN_KEYWORD, ("inflação -câmbio",))} == set()
    back = lexical.rebuild(db, target=lexical.LEGACY)
    assert back["rebuilt"] == back["pending"] > 0 and back["remaining"] == 0
    assert {r["item_id"] for r in db.rows(MAIN_KEYWORD, ("inflação -câmbio",))} == {b}
    assert {r["v"] for r in db.rows("SELECT search_version AS v FROM passages")} == {lexical.LEGACY}
    assert lexical.rebuild(db, target=lexical.LEGACY, dry_run=True)["pending"] == 0
    # Rolling forward again: the migration rewrites every vector before search serves.
    forward = db.migrate()
    assert forward["rebuilt"] == back["rebuilt"] and forward["remaining"] == 0
    assert keyword(db, "inflacao -cambio") == [b] and set(keyword(db, "inflação")) == {a, b}
    with pytest.raises(ValueError, match="^rebuild target must be lexical-v1 or legacy$"):
        lexical.rebuild(db, target="v0")


def test_one_document_with_many_matches_does_not_starve_the_others(research):
    from ofl.research.search import search

    db, _ = research
    big = item(db, "big", "Nota", "Selic juros.")
    with db.connect() as con:
        for n in range(150):
            text = f"Selic e juros, parte {n}."
            con.execute(
                f"""INSERT INTO passages(id,item_id,ordinal,text,revision,method,search,search_version)
                VALUES (%s,%s,%s,%s,'big-many','collector_news',{lexical.DOCUMENT},%s)""",
                (f"big-{n}", big, n + 10, text, *document_params(text, "ocr"), lexical.VERSION),
            )
    others = [item(db, f"o{n}", "Nota", f"A Selic, nota {n}.") for n in range(5)]
    found = [r["item_id"] for r in search(db, "Selic juros", mode="keyword", limit=20)["results"]]
    assert found[0] == big and set(found[1:]) == set(others)


def test_migration_leaves_no_stale_vector(research):
    db, _ = research
    item(db, "a", "Inflação", "A inflação sobe.")
    with db.connect() as con:
        con.execute("UPDATE passages SET search=to_tsvector('portuguese',text),search_version=NULL")
    result = db.migrate()
    assert result["remaining"] == 0 and result["rebuilt"] == result["pending"] > 0
    assert (
        db.rows(
            "SELECT count(*) AS n FROM passages WHERE search_version IS DISTINCT FROM %s",
            (lexical.VERSION,),
        )[0]["n"]
        == 0
    )
    assert db.migrate() == {
        "lexical": lexical.VERSION,
        "pending": 0,
        "rebuilt": 0,
        "remaining": 0,
    }


def test_migration_fails_while_a_vector_is_stale(research, monkeypatch):
    db, _ = research
    stale = {"lexical": lexical.VERSION, "pending": 3, "rebuilt": 2, "remaining": 1}
    monkeypatch.setattr(lexical, "rebuild", lambda database: stale)
    with pytest.raises(RuntimeError, match="^1 passage vectors are not in lexical-v1 yet"):
        db.migrate()


def test_rebuild_cli_fails_while_a_locked_passage_stays_behind(research, monkeypatch, capsys):
    import argparse
    import json

    import psycopg

    from ofl.research.cli import configure, dispatch

    db, store = research
    item(db, "a", "Inflação", "A inflação sobe.")
    for key, value in db.settings.model_dump().items():
        if value is not None and not isinstance(value, (dict, list)):
            monkeypatch.setenv(f"OFL_RESEARCH_{key.upper()}", str(value))
    parser = argparse.ArgumentParser()
    configure(parser.add_subparsers(dest="cmd"))

    def run(*options):
        return dispatch(parser.parse_args(["research", "rebuild-search", *options]))

    locked = db.rows("SELECT id FROM passages ORDER BY id LIMIT 1")[0]["id"]
    with psycopg.connect(db.settings.database_url) as holder:
        holder.execute("SELECT 1 FROM passages WHERE id=%s FOR UPDATE", (locked,))
        assert run("--target", "legacy") == 1
    out = capsys.readouterr()
    assert json.loads(out.out.strip().splitlines()[-1])["remaining"] == 1
    assert "pause passage writers" in out.err
    assert run("--target", "legacy", "--dry-run") == 0
    assert run("--target", "legacy") == 0
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["remaining"] == 0
