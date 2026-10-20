"""Offline evidence-integrity contracts: review binding, dataset bundles and their replay."""

import csv
import hashlib
import io
import json
import zipfile
from datetime import date
from decimal import Decimal

import pytest

pytest.importorskip("psycopg")

from ofl.research import dataset_bundle, dataset_replay, macro, query
from ofl.research.analytics import prepare_dataset, release_diff
from ofl.research.db import digest
from ofl.research.reference_review import reference_reviews, text_hash
from ofl.research.workspaces import evidence, restricted, validate

# Reference reviews bound to the question and the captured source


QUESTION = "Did the policy rate change in March?"
CAPTURED = [
    {"id": "p1", "text": "The committee raised the rate.", "revision": 1},
    {"id": "p2", "text": "Inflation stayed high.", "revision": 3},
]


def review(passage="p1", **extra):
    return {"passage_id": passage, "verdict": "relevant", "rationale": "states it", **extra}


def test_question_hash_is_the_sha256_of_its_utf8_text():
    assert text_hash("ação") == hashlib.sha256("ação".encode()).hexdigest()


def test_reviews_record_reviewer_and_the_hashes_they_were_made_against():
    result = reference_reviews(
        QUESTION,
        CAPTURED,
        [review(rationale="  states it  "), review("p2", verdict="counterevidence")],
        "ana",
    )
    question = hashlib.sha256(QUESTION.encode()).hexdigest()
    assert result == [
        {
            "passage_id": "p1",
            "verdict": "relevant",
            "rationale": "states it",
            "reviewer": "ana",
            "question_sha256": question,
            "source_sha256": digest(CAPTURED[0]),
            "status": "author_judgment_not_independently_verified",
        },
        {
            "passage_id": "p2",
            "verdict": "counterevidence",
            "rationale": "states it",
            "reviewer": "ana",
            "question_sha256": question,
            "source_sha256": digest(CAPTURED[1]),
            "status": "author_judgment_not_independently_verified",
        },
    ]


def test_resubmitted_reviews_with_current_hashes_are_kept():
    first = reference_reviews(QUESTION, CAPTURED, [review()], "ana")
    again = reference_reviews(QUESTION, CAPTURED, first, "rui")
    assert again == [{**first[0], "reviewer": "rui"}]


@pytest.mark.parametrize("verdict", ["relevant", "counterevidence", "irrelevant", "uncertain"])
def test_each_documented_verdict_is_accepted(verdict):
    assert (
        reference_reviews(QUESTION, CAPTURED, [review(verdict=verdict)], "a")[0]["verdict"]
        == verdict
    )


def test_up_to_50_reviews_and_1000_character_rationales_are_accepted():
    captured = [{"id": f"p{i}", "text": str(i)} for i in range(50)]
    proposed = [review(f"p{i}", rationale="r" * 1000) for i in range(50)]
    assert len(reference_reviews(QUESTION, captured, proposed, "a")) == 50
    assert (
        reference_reviews(QUESTION, CAPTURED, [review(rationale="x")], "a")[0]["rationale"] == "x"
    )


@pytest.mark.parametrize(
    "proposed,message",
    [
        ({"p1": review()}, "at most 50 reference reviews"),
        ([review()] * 51, "at most 50 reference reviews"),
        (["p1"], "invalid reference review"),
        ([review("p9")], "review must cite one distinct captured reference"),
        ([review(1)], "review must cite one distinct captured reference"),
        ([review(), review()], "review must cite one distinct captured reference"),
        ([review(verdict="supports")], "reference review needs a verdict and a bounded rationale"),
        ([review(verdict=None)], "reference review needs a verdict and a bounded rationale"),
        ([review(rationale="   ")], "reference review needs a verdict and a bounded rationale"),
        ([review(rationale=5)], "reference review needs a verdict and a bounded rationale"),
        (
            [review(rationale="r" * 1001)],
            "reference review needs a verdict and a bounded rationale",
        ),
        (
            [review(question_sha256=text_hash("Another question"))],
            "review is stale: question or source changed; review this reference again",
        ),
        (
            [review(source_sha256=digest({**CAPTURED[0], "revision": 2}))],
            "review is stale: question or source changed; review this reference again",
        ),
    ],
)
def test_invalid_or_stale_reviews_are_rejected(proposed, message):
    with pytest.raises(ValueError, match=f"^{message}$"):
        reference_reviews(QUESTION, CAPTURED, proposed, "ana")


# Investigation validation with captured evidence


class Rows:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class Passages:
    """Serves passages by id with their current activity, as the evidence query would.

    A row is an active passage without an asset unless it sets passage_active or
    asset_active itself.
    """

    def __init__(self, passages):
        self.passages = passages
        self.calls = []

    def execute(self, sql, params):
        assert sql.rstrip().endswith("WHERE p.id=%s")
        assert "p.active AS passage_active,a.active AS asset_active" in sql
        self.calls.append(params)
        row = self.passages.get(params[0])
        flags = {"passage_active": True, "asset_active": None}
        return Rows(row and {**flags, **row})


PASSAGES = {
    "p1": {
        "id": "p1",
        "text": "The committee raised the Selic rate to 10.75%.",
        "revision": 2,
        "date_start": date(2024, 3, 20),
        "value": Decimal("10.75"),
    },
    "p2": {"id": "p2", "text": "Inflation expectations rose.", "revision": 1},
}


def investigation(**extra):
    return {
        "question": QUESTION,
        "references": ["p1", "p2"],
        "findings": [
            {"kind": "observation", "text": "raised the Selic rate", "references": ["p1"]},
            {"kind": "interpretation", "text": "Policy tightened.", "references": ["p1", "p2"]},
        ],
        **extra,
    }


def test_evidence_is_captured_in_reference_order_as_json_values():
    con = Passages(PASSAGES)
    captured = evidence(con, ["p2", "p1"])
    assert con.calls == [("p2",), ("p1",)]
    assert captured == [
        PASSAGES["p2"],
        {**PASSAGES["p1"], "date_start": "2024-03-20", "value": "10.75"},
    ]
    assert evidence(con, []) == []
    assert (
        len(
            evidence(
                Passages({f"x{i}": {"id": i} for i in range(50)}), [f"x{i}" for i in range(50)]
            )
        )
        == 50
    )


@pytest.mark.parametrize(
    "references,message",
    [
        ([f"x{i}" for i in range(51)], "provide at most 50 distinct passage references"),
        (["p1", "p1"], "provide at most 50 distinct passage references"),
        (["p1", "gone"], "evidence reference unavailable"),
    ],
)
def test_evidence_references_must_be_distinct_and_available(references, message):
    with pytest.raises(ValueError, match=f"^{message}$"):
        evidence(Passages(PASSAGES), references)


NEWS = {"source": "news", "metadata": {"content_tier": "headline_only"}}
SUPERSEDED = {
    "old": {"id": "old", "text": "Old page text.", "passage_active": False, "source": "mub3"},
    "asset": {"id": "asset", "text": "Old asset.", "asset_active": False, "source": "mub3"},
    "withheld": {"id": "withheld", "text": "Body.", "passage_active": False, **NEWS},
    "body": {
        "id": "body",
        "text": "Body.",
        "passage_active": False,
        "source": "news",
        "metadata": {"content_tier": "full_text"},
    },
    "headline": {"id": "headline", "text": "Headline.", "asset_active": True, **NEWS},
}


def test_superseded_passages_stay_capturable_only_for_previous_references():
    con = Passages(SUPERSEDED)
    previous = {"old", "asset", "withheld", "body", "headline"}
    captured = evidence(con, ["old", "asset", "body", "headline"], previous)
    assert [c["id"] for c in captured] == ["old", "asset", "body", "headline"]
    assert all("passage_active" not in c and "asset_active" not in c for c in captured)
    assert evidence(con, ["headline"]) == [captured[3]]
    for passage in ("old", "asset", "body"):
        with pytest.raises(ValueError, match="^evidence reference unavailable$"):
            evidence(con, [passage], previous - {passage})
    # The current policy withholds a superseded body of a now headline-only item.
    with pytest.raises(ValueError, match="^evidence reference unavailable$"):
        evidence(con, ["withheld"], previous)
    with pytest.raises(ValueError, match="^evidence reference unavailable$"):
        evidence(con, ["gone"], {"gone"})


def test_validate_passes_previous_references_to_capture():
    body = {"question": "q", "references": ["old"]}
    assert validate(Passages(SUPERSEDED), "investigation", body, previous={"old"})["evidence"]
    with pytest.raises(ValueError, match="^evidence reference unavailable$"):
        validate(Passages(SUPERSEDED), "investigation", body)


@pytest.mark.parametrize(
    ("row", "withheld"),
    [
        ({"passage_active": False, "asset_active": None, **NEWS}, True),
        ({"passage_active": True, "asset_active": False, **NEWS}, True),
        ({"passage_active": True, "asset_active": None, **NEWS}, False),
        ({"passage_active": True, "asset_active": True, **NEWS}, False),
        (
            {
                "passage_active": False,
                "asset_active": None,
                "source": "mub3",
                "metadata": NEWS["metadata"],
            },
            False,
        ),
        ({"passage_active": False, "asset_active": None, "source": "news", "metadata": {}}, False),
    ],
)
def test_current_news_policy_restricts_superseded_text(row, withheld):
    assert restricted(row) is withheld


def test_investigation_findings_are_labeled_and_reviews_bound_server_side():
    body = investigation(reference_reviews=[review("p2", verdict="uncertain")])
    result = validate(Passages(PASSAGES), "investigation", body, reviewer="ana")
    assert [f["status"] for f in result["findings"]] == [
        "source_excerpt",
        "user_authored_unverified",
    ]
    assert result["evidence"] == evidence(Passages(PASSAGES), ["p1", "p2"])
    assert result["reference_reviews"] == reference_reviews(
        QUESTION, result["evidence"], body["reference_reviews"], "ana"
    )
    assert result["question"] == QUESTION and result["references"] == ["p1", "p2"]


def test_investigation_defaults_to_no_references_findings_or_reviews():
    result = validate(Passages({}), "investigation", {"question": "q"})
    assert result == {"question": "q", "evidence": [], "findings": [], "reference_reviews": []}


def test_investigation_bounds_are_inclusive():
    body = investigation(question="q" * 2000)
    body["findings"] = [{"kind": "limitation", "text": "t" * 4000}] * 50
    assert len(validate(Passages(PASSAGES), "investigation", body)["findings"]) == 50
    body["findings"] = [{"kind": "counterpoint", "text": "t"}]
    assert validate(Passages(PASSAGES), "investigation", body)["findings"][0]["status"] == (
        "user_authored_unverified"
    )


@pytest.mark.parametrize(
    "change,message",
    [
        (lambda b: b.pop("question"), "investigation needs a question"),
        (lambda b: b.update(question=""), "investigation needs a question"),
        (lambda b: b.update(question="q" * 2001), "investigation needs a question"),
        (lambda b: b.update(question=["q"]), "investigation needs a question"),
        (lambda b: b.update(references="p1"), "invalid references"),
        (lambda b: b.update(references=["p1", 2]), "invalid references"),
        (lambda b: b.update(findings={}), "at most 50 findings"),
        (lambda b: b.update(findings=[b["findings"][1]] * 51), "at most 50 findings"),
        (lambda b: b["findings"].append("note"), "finding kind required"),
        (lambda b: b["findings"][0].pop("kind"), "finding kind required"),
        (lambda b: b["findings"][0].update(kind="claim"), "finding kind required"),
        (lambda b: b["findings"][0].update(kind=["observation"]), "finding kind required"),
        (lambda b: b["findings"][1].pop("text"), "finding text required"),
        (lambda b: b["findings"][1].update(text=""), "finding text required"),
        (lambda b: b["findings"][1].update(text="t" * 4001), "finding text required"),
        (lambda b: b["findings"][1].update(text=7), "finding text required"),
        (lambda b: b["findings"][1].update(references="p1"), "finding cites uncaptured evidence"),
        (lambda b: b["findings"][1].update(references=["p3"]), "finding cites uncaptured evidence"),
        (lambda b: b["findings"][1].update(references=[1]), "finding cites uncaptured evidence"),
        (
            lambda b: b["findings"][0].update(references=[]),
            "observations must quote captured source text exactly; use interpretation otherwise",
        ),
        (
            lambda b: b["findings"][0].pop("references"),
            "observations must quote captured source text exactly; use interpretation otherwise",
        ),
        (
            lambda b: b["findings"][0].update(text="raised the selic rate"),
            "observations must quote captured source text exactly; use interpretation otherwise",
        ),
        (
            lambda b: b["findings"][0].update(references=["p2"]),
            "observations must quote captured source text exactly; use interpretation otherwise",
        ),
        (
            lambda b: b.update(reference_reviews=[review("p3")]),
            "review must cite one distinct captured reference",
        ),
    ],
)
def test_investigation_rejects_unsupported_findings(change, message):
    body = investigation()
    change(body)
    with pytest.raises(ValueError, match=f"^{message}$"):
        validate(Passages(PASSAGES), "investigation", body)


def test_an_observation_may_quote_any_one_of_its_sources():
    body = investigation()
    body["findings"][0]["references"] = ["p2", "p1"]
    assert validate(Passages(PASSAGES), "investigation", body)["findings"][0]["status"] == (
        "source_excerpt"
    )


def test_artifacts_are_bounded_to_500_kb():
    with pytest.raises(ValueError, match="^artifact exceeds 500KB$"):
        validate(None, "check", {"type": "coverage", "pad": "é" * 500000})
    exact = {"type": "coverage", "pad": ""}
    exact["pad"] = "x" * (500000 - len(json.dumps(exact)))
    assert validate(None, "check", exact) == exact
    exact["pad"] += "x"
    with pytest.raises(ValueError, match="^artifact exceeds 500KB$"):
        validate(None, "check", exact)


def test_checks_drop_client_supplied_results_unless_generated():
    body = {"type": "coverage", "result": 1, "comparison": 2, "executed_at": 3, "note": "n"}
    assert validate(None, "check", body) == {"type": "coverage", "note": "n"}
    assert validate(None, "check", body, generated=True) == body
    assert validate(None, "check", {"type": "search", "q": "q" * 1000})["q"] == "q" * 1000
    assert validate(None, "check", {"type": "search", "q": "q"})["q"] == "q"


@pytest.mark.parametrize(
    "body,message",
    [
        ({"type": "alert"}, "check type must be search or coverage"),
        ({"type": ["search"]}, "check type must be search or coverage"),
        ({}, "check type must be search or coverage"),
        ({"type": "search"}, "search check needs a bounded query"),
        ({"type": "search", "q": ""}, "search check needs a bounded query"),
        ({"type": "search", "q": "q" * 1001}, "search check needs a bounded query"),
        ({"type": "search", "q": 5}, "search check needs a bounded query"),
    ],
)
def test_checks_need_a_known_type_and_bounded_query(body, message):
    with pytest.raises(ValueError, match=f"^{message}$"):
        validate(None, "check", body)


def watch(**extra):
    return {
        "isin": "BRPETRACNPR6",
        "market": "010",
        "currency": "BRL",
        "distribution": "100",
        **extra,
    }


def test_watchlists_need_full_instrument_identity():
    body = {"entries": [watch()] * 100}
    assert validate(None, "watchlist", body) is body
    message = "^ISIN, market, currency and distribution are required; ticker alone is insufficient$"
    for entry in (
        {"ticker": "PETR4"},
        watch(isin=" "),
        watch(market=None),
        watch(currency=1),
        {k: v for k, v in watch().items() if k != "distribution"},
        "PETR4",
    ):
        with pytest.raises(ValueError, match=message):
            validate(None, "watchlist", {"entries": [entry]})
    for entries in ([], [watch()] * 101, {"a": watch()}, None):
        with pytest.raises(ValueError, match="^watchlist needs 1–100 instrument identities$"):
            validate(None, "watchlist", {"entries": entries} if entries is not None else {})


def test_portfolio_and_dataset_artifacts_are_validated_by_their_analytics():
    body = {
        "currency": "BRL",
        "valuation_date": "2026-10-18",
        "positions": [
            {"instrument_id": "A", "market": "010", "currency": "BRL", "market_value": "2"}
        ],
    }
    result = validate(None, "portfolio", body)
    assert result["analysis"]["total_value"] == "2" and result["positions"] == body["positions"]
    spec = {"snapshot_id": "s1", "series": [{"series_id": "ipca"}], "date_from": "2024-01-01"}
    assert validate(None, "dataset", spec) == {
        "snapshot_id": "s1",
        "series": [{"series_id": "ipca", "operation": "level", "window": 3, "lag": 1}],
        "date_from": "2024-01-01",
        "date_to": None,
        "alignment": "outer",
    }
    with pytest.raises(ValueError, match="^unsupported workspace kind$"):
        validate(None, "note", {})


# Dataset preparation, bundle and offline replay

SERIES = {
    "ipca": (
        "%",
        [
            ("2024-01-01", "1"),
            ("2024-02-01", "2"),
            ("2024-03-01", "4"),
            ("2024-04-01", None),
            ("2024-05-01", "8"),
            ("2024-06-01", "3"),
        ],
    ),
    "rates": ("index", [("2024-02-01", "10"), ("2024-03-01", "0"), ("2024-05-01", "5")]),
    "zero": ("BRL", [("2024-01-01", "0"), ("2024-02-01", "2")]),
    "nulls": ("BRL", [("2024-01-01", None), ("2024-02-01", "2"), ("2024-03-01", "6")]),
    "empty": ("BRL", [("2024-01-01", None), ("2024-02-01", None)]),
}


def fake_series(store_calls):
    def series_query(db, store, series_id, *, snapshot_id, date_from, date_to, limit):
        store_calls.append((db, store, series_id, snapshot_id, date_from, date_to, limit))
        unit, rows = SERIES[series_id]
        selected = [
            {"date": date.fromisoformat(day), "value": value}
            for day, value in rows
            if (not date_from or day >= str(date_from)) and (not date_to or day <= str(date_to))
        ]
        return {
            "snapshot_id": snapshot_id,
            "snapshot_generated_at": date(2024, 7, 1),
            "series": {"key": series_id, "unit": unit},
            "rows": selected,
            "offset": 0,
            "has_more": False,
            "source_dataset_sha256": "sha-" + series_id,
        }

    return series_query


@pytest.fixture
def sources(monkeypatch):
    calls = []
    monkeypatch.setattr(macro, "series_query", fake_series(calls))
    return calls


def table(content):
    return list(csv.reader(io.StringIO(content)))


@pytest.mark.parametrize(
    "operation,options,expected",
    [
        ("level", {}, ["1", "2", "4", "", "8", "3"]),
        ("difference", {}, ["", "1", "2", "", "", "-5"]),
        ("difference", {"lag": 2}, ["", "", "3", "", "4", ""]),
        ("percent_change", {}, ["", "100", "100", "", "", "-62.500"]),
        ("percent_change", {"lag": 2}, ["", "", "300", "", "100", ""]),
        ("index_100", {}, ["100", "200", "400", "", "800", "300"]),
        ("rolling_mean", {"window": 2}, ["", "1.5", "3", "", "", "5.5"]),
        ("rolling_mean", {"window": 3}, ["", "", "2." + "3" * 49, "", "", ""]),
    ],
)
def test_transformations_are_exact_and_never_fill_gaps(sources, operation, options, expected):
    spec = {
        "snapshot_id": "s1",
        "series": [{"series_id": "ipca", "operation": operation, **options}],
    }
    prepared = prepare_dataset("db", "store", spec)
    rows = table(prepared["csv"])
    assert rows[0] == ["date", f"ipca__{operation}"]
    assert [r[1] for r in rows[1:]] == expected
    assert sources == [("db", "store", "ipca", "s1", None, None, 100000)]


def test_zero_bases_produce_no_percent_or_index_values(sources):
    spec = {
        "snapshot_id": "s1",
        "series": [
            {"series_id": "zero", "operation": "percent_change"},
            {"series_id": "zero", "operation": "index_100"},
            {"series_id": "nulls", "operation": "index_100"},
            {"series_id": "rates", "operation": "percent_change"},
        ],
    }
    rows = table(prepare_dataset("db", "store", spec)["csv"])
    assert rows == [
        [
            "date",
            "zero__percent_change",
            "zero__index_100",
            "nulls__index_100",
            "rates__percent_change",
        ],
        ["2024-01-01", "", "", "", ""],
        ["2024-02-01", "", "", "100", ""],
        ["2024-03-01", "", "", "300", "-100"],
        ["2024-05-01", "", "", "", ""],
    ]


def test_alignment_manifest_and_units(sources):
    spec = {
        "snapshot_id": "s1",
        "alignment": "inner",
        "date_from": "2024-02-01",
        "date_to": "2024-05-01",
        "series": [
            {"series_id": "ipca", "operation": "difference"},
            {"series_id": "rates", "operation": "index_100"},
        ],
    }
    prepared = prepare_dataset("db", "store", spec)
    expected_csv = (
        "date,ipca__difference,rates__index_100\r\n"
        "2024-02-01,,100\r\n2024-03-01,2,0\r\n2024-05-01,,50.0\r\n"
    )
    assert prepared["csv"] == expected_csv
    assert prepared["preview"] == [
        {"date": "2024-02-01", "ipca__difference": None, "rates__index_100": "100"},
        {"date": "2024-03-01", "ipca__difference": "2", "rates__index_100": "0"},
        {"date": "2024-05-01", "ipca__difference": None, "rates__index_100": "50.0"},
    ]
    manifest = prepared["manifest"]
    assert manifest["rows"] == 3
    assert manifest["csv_sha256"] == hashlib.sha256(expected_csv.encode()).hexdigest()
    assert manifest["method"] == "ofl/descriptive-dataset/1"
    assert manifest["specification"]["date_from"] == "2024-02-01"
    assert manifest["columns"] == [
        {
            "column": "ipca__difference",
            "source": {"key": "ipca", "unit": "%"},
            "source_dataset_sha256": "sha-ipca",
            "operation": {"series_id": "ipca", "operation": "difference", "window": 3, "lag": 1},
            "output_unit": "percentage_points",
        },
        {
            "column": "rates__index_100",
            "source": {"key": "rates", "unit": "index"},
            "source_dataset_sha256": "sha-rates",
            "operation": {"series_id": "rates", "operation": "index_100", "window": 3, "lag": 1},
            "output_unit": "index",
        },
    ]
    assert manifest["null_policy"].startswith("No imputation.")
    assert manifest["qualification"].startswith("Macro source numeric representation")
    outer = prepare_dataset("db", "store", {**spec, "alignment": "outer"})
    assert outer["manifest"]["rows"] == 4


@pytest.mark.parametrize(
    "series_id,operation,unit",
    [
        ("ipca", "percent_change", "%"),
        ("zero", "difference", "BRL"),
        ("zero", "level", "BRL"),
        ("zero", "percent_change", "%"),
        ("zero", "index_100", "index"),
        ("rates", "difference", "index"),
        ("ipca", "rolling_mean", "%"),
    ],
)
def test_output_units_follow_the_transformation(sources, series_id, operation, unit):
    spec = {"snapshot_id": "s1", "series": [{"series_id": series_id, "operation": operation}]}
    assert prepare_dataset("db", "store", spec)["manifest"]["columns"][0]["output_unit"] == unit


@pytest.mark.parametrize("unit", ["%", "Percent", "pct", "PERCENTUAL"])
def test_differences_of_percentages_are_percentage_points(sources, monkeypatch, unit):
    monkeypatch.setitem(SERIES, "pp", (unit, [("2024-01-01", "1")]))
    spec = {"snapshot_id": "s1", "series": [{"series_id": "pp", "operation": "difference"}]}
    assert prepare_dataset("db", "store", spec)["manifest"]["columns"][0]["output_unit"] == (
        "percentage_points"
    )


def test_preview_is_the_first_200_rows(sources, monkeypatch):
    days = [(date.fromordinal(738000 + i).isoformat(), str(i)) for i in range(201)]
    monkeypatch.setitem(SERIES, "long", ("BRL", days))
    prepared = prepare_dataset(
        "db", "store", {"snapshot_id": "s1", "series": [{"series_id": "long"}]}
    )
    assert len(prepared["preview"]) == 200 and prepared["manifest"]["rows"] == 201
    assert prepared["preview"][0] == {"date": days[0][0], "long__level": "0"}


@pytest.mark.parametrize(
    "spec,message",
    [
        (
            {
                "snapshot_id": "s1",
                "date_from": "2024-02-02",
                "date_to": "2024-02-01",
                "series": [{"series_id": "ipca"}],
            },
            "date interval reversed",
        ),
        (
            {"snapshot_id": "s1", "series": [{"series_id": "ipca"}, {"series_id": "ipca"}]},
            "duplicate output columns",
        ),
    ],
)
def test_dataset_specifications_are_checked(sources, spec, message):
    with pytest.raises(ValueError, match=f"^{message}$"):
        prepare_dataset("db", "store", spec)


def test_same_day_interval_is_allowed(sources):
    spec = {
        "snapshot_id": "s1",
        "date_from": "2024-02-01",
        "date_to": "2024-02-01",
        "series": [{"series_id": "ipca"}],
    }
    assert prepare_dataset("db", "store", spec)["manifest"]["rows"] == 1


def test_truncated_sources_are_refused(monkeypatch):
    def truncated(*args, **kwargs):
        return {"has_more": True}

    monkeypatch.setattr(macro, "series_query", truncated)
    with pytest.raises(ValueError, match="^dataset exceeds 100,000 source rows; narrow dates$"):
        prepare_dataset("db", "store", {"snapshot_id": "s1", "series": [{"series_id": "x"}]})


def test_aligned_rows_are_bounded(sources, monkeypatch):
    import ofl.research.analytics as analytics

    days = [(date.fromordinal(700000 + i).isoformat(), "1") for i in range(100001)]
    monkeypatch.setitem(SERIES, "big", ("BRL", days))
    with pytest.raises(ValueError, match="^aligned dataset exceeds 100,000 rows$"):
        analytics.prepare_dataset(
            "db", "store", {"snapshot_id": "s1", "series": [{"series_id": "big"}]}
        )
    monkeypatch.setitem(SERIES, "big", ("BRL", days[:100000]))
    spec = {"snapshot_id": "s1", "series": [{"series_id": "big"}]}
    assert analytics.prepare_dataset("db", "store", spec)["manifest"]["rows"] == 100000


SPEC = {
    "snapshot_id": "s1",
    "series": [
        {"series_id": "ipca", "operation": "rolling_mean", "window": 3},
        {"series_id": "rates", "operation": "percent_change"},
    ],
}

README = (
    "OFL reproducible descriptive dataset\n\n"
    "Extract this archive into a new directory. Inspect reproduce.py and run:\n"
    "  python3 reproduce.py\n"
    "Requires Python 3.11+, standard library only. No network, database or model.\n"
    "The script verifies inputs and recomputes dataset.reproduced.csv.\n"
    "Existing output files are never overwritten.\n"
    "Retain receipt.json from the authenticated export.\n"
    "Hashes detect changes; they are not signatures.\n"
    "Observation dates are not publication dates. No imputation or hidden warm-up.\n"
    "This is not a backtest or an independent verification of source correctness.\n"
)


def unpack(raw, folder):
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        archive.extractall(folder)
        return {info.filename: info for info in archive.infolist()}


def test_bundle_members_receipt_and_fixed_archive_metadata(sources, tmp_path):
    raw = dataset_bundle.bundle("db", "store", SPEC)
    assert raw == dataset_bundle.bundle("db", "store", SPEC)
    infos = unpack(raw, tmp_path)
    assert list(infos) == [
        "inputs.json",
        "manifest.json",
        "dataset.csv",
        "reproduce.py",
        "README.txt",
        "receipt.json",
    ]
    for info in infos.values():
        assert info.date_time == (1980, 1, 1, 0, 0, 0)
        assert info.compress_type == zipfile.ZIP_DEFLATED
        assert info.external_attr == 0o600 << 16
    prepared = prepare_dataset("db", "store", SPEC)
    assert (tmp_path / "dataset.csv").read_bytes() == prepared["csv"].encode()
    assert json.loads((tmp_path / "manifest.json").read_text()) == prepared["manifest"]
    assert (tmp_path / "manifest.json").read_text() == json.dumps(
        prepared["manifest"], ensure_ascii=False, indent=2
    )
    captured = json.loads((tmp_path / "inputs.json").read_text())
    assert [c["series"]["key"] for c in captured] == ["ipca", "rates"]
    assert (tmp_path / "reproduce.py").read_bytes() == (
        __import__("importlib.resources")
        .resources.files("ofl.research")
        .joinpath("dataset_replay.py")
        .read_bytes()
    )
    assert (tmp_path / "README.txt").read_text() == README
    receipt = json.loads((tmp_path / "receipt.json").read_text())
    assert (tmp_path / "receipt.json").read_text() == json.dumps(receipt, indent=2)
    assert receipt == {
        "version": 1,
        "sha256": {
            name: hashlib.sha256((tmp_path / name).read_bytes()).hexdigest()
            for name in (
                "inputs.json",
                "manifest.json",
                "dataset.csv",
                "reproduce.py",
                "README.txt",
            )
        },
    }


def test_bundle_keeps_non_ascii_text_readable(sources, tmp_path, monkeypatch):
    monkeypatch.setitem(SERIES, "ipca", ("R$ milhões", [("2024-01-01", "1")]))
    raw = dataset_bundle.bundle(
        "db", "store", {"snapshot_id": "s1", "series": [{"series_id": "ipca"}]}
    )
    unpack(raw, tmp_path)
    assert "R$ milhões" in (tmp_path / "inputs.json").read_text(encoding="utf-8")
    assert "R$ milhões" in (tmp_path / "manifest.json").read_text(encoding="utf-8")


def test_bundle_size_bound_is_inclusive(sources, monkeypatch):
    raw = dataset_bundle.bundle("db", "store", SPEC)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        total = sum(
            info.file_size for info in archive.infolist() if info.filename != "receipt.json"
        )
    monkeypatch.setattr(dataset_bundle, "MAX_BYTES", total)
    dataset_bundle.bundle("db", "store", SPEC)
    monkeypatch.setattr(dataset_bundle, "MAX_BYTES", total - 1)
    with pytest.raises(
        ValueError, match="^reproduction bundle exceeds 64 MiB; narrow dates or series$"
    ):
        dataset_bundle.bundle("db", "store", SPEC)


def test_bundle_names_the_resource_package_explicitly(sources, monkeypatch):
    # Python 3.11 has no caller-relative default for importlib.resources.files().
    anchors = []
    real = dataset_bundle.files

    def recording(anchor):
        anchors.append(anchor)
        return real(anchor)

    monkeypatch.setattr(dataset_bundle, "files", recording)
    dataset_bundle.bundle("db", "store", SPEC)
    assert anchors == ["ofl.research"]


def test_bundle_limit_is_64_mib():
    assert dataset_bundle.MAX_BYTES == 64 * 1024**2 == dataset_replay.MAX_BYTES


@pytest.mark.parametrize("alignment", ["inner", "outer"])
@pytest.mark.parametrize(
    "operation,options",
    [
        ("level", {}),
        ("difference", {"lag": 2}),
        ("percent_change", {}),
        ("index_100", {}),
        ("rolling_mean", {"window": 2}),
    ],
)
def test_offline_replay_recomputes_the_server_dataset(
    sources, tmp_path, alignment, operation, options
):
    spec = {
        "snapshot_id": "s1",
        "alignment": alignment,
        "date_from": "2024-01-15",
        "series": [
            {"series_id": "ipca", "operation": operation, **options},
            {"series_id": "rates", "operation": operation, **options},
            {"series_id": "nulls", "operation": operation, **options},
        ],
    }
    unpack(dataset_bundle.bundle("db", "store", spec), tmp_path)
    assert dataset_replay.replay(tmp_path) == (tmp_path / "dataset.csv").read_bytes()


def bundle_folder(tmp_path, spec=SPEC):
    unpack(dataset_bundle.bundle("db", "store", spec), tmp_path)
    return tmp_path


def rewrite(folder, name, change):
    """Edit one member and its receipt hash, as a consistent but altered bundle."""
    path = folder / name
    if name.endswith(".json"):
        value = json.loads(path.read_text())
        change(value)
        path.write_text(json.dumps(value))
    else:
        path.write_bytes(change(path.read_bytes()))
    receipt = json.loads((folder / "receipt.json").read_text())
    receipt["sha256"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (folder / "receipt.json").write_text(json.dumps(receipt))


def recipe(change):
    return lambda manifest: change(manifest["specification"])


def first_source(change):
    return lambda inputs: change(inputs[0])


def first_rows(change):
    return lambda inputs: change(inputs[0]["rows"])


@pytest.mark.parametrize(
    "name,change,message",
    [
        ("receipt.json", lambda r: r.update(version=2), "unsupported receipt version"),
        (
            "manifest.json",
            lambda m: m.update(method="ofl/descriptive-dataset/2"),
            "unsupported transformation method",
        ),
        ("manifest.json", recipe(lambda s: s.update(series=[])), "input/recipe series mismatch"),
        (
            "manifest.json",
            recipe(lambda s: s.update(series=s["series"] * 5)),
            "input/recipe series mismatch",
        ),
        ("inputs.json", lambda i: i.pop(), "input/recipe series mismatch"),
        (
            "manifest.json",
            recipe(lambda s: s.update(snapshot_id="s2")),
            "input identity differs from pinned recipe",
        ),
        (
            "inputs.json",
            first_source(lambda s: s.update(snapshot_id="s2")),
            "input identity differs from pinned recipe",
        ),
        (
            "inputs.json",
            first_source(lambda s: s["series"].update(key="rates")),
            "input identity differs from pinned recipe",
        ),
        (
            "inputs.json",
            first_source(lambda s: s.update(has_more=True)),
            "incomplete captured source query",
        ),
        (
            "inputs.json",
            first_source(lambda s: s.update(offset=1)),
            "incomplete captured source query",
        ),
        (
            "inputs.json",
            first_rows(lambda r: r.reverse()),
            "input dates must be unique and ordered",
        ),
        (
            "inputs.json",
            first_rows(lambda r: r.append(dict(r[-1]))),
            "input dates must be unique and ordered",
        ),
        (
            "manifest.json",
            recipe(lambda s: s.update(date_from="2024-02-01")),
            "input date outside recipe",
        ),
        (
            "manifest.json",
            recipe(lambda s: s.update(date_to="2024-05-31")),
            "input date outside recipe",
        ),
        ("inputs.json", first_rows(lambda r: r[0].update(value="NaN")), "non-finite input"),
        ("inputs.json", first_rows(lambda r: r[0].update(value="-Infinity")), "non-finite input"),
        (
            "manifest.json",
            recipe(lambda s: s["series"][0].update(operation="sum")),
            "unsupported operation",
        ),
        ("manifest.json", recipe(lambda s: s.update(alignment="left")), "unsupported alignment"),
        (
            "manifest.json",
            lambda m: m.update(rows=m["rows"] + 1),
            "recomputed result differs from expected rows/checksum",
        ),
        (
            "manifest.json",
            lambda m: m.update(csv_sha256="0" * 64),
            "recomputed result differs from expected rows/checksum",
        ),
        (
            "inputs.json",
            first_rows(lambda r: r[1].update(value="2.5")),
            "recomputed result differs from expected rows/checksum",
        ),
        (
            "dataset.csv",
            lambda raw: raw.replace(b"\r\n", b"\n"),
            "recomputed bytes differ from bundled dataset",
        ),
    ],
)
def test_replay_rejects_altered_bundles(sources, tmp_path, name, change, message):
    folder = bundle_folder(tmp_path)
    if name == "receipt.json":
        receipt = json.loads((folder / name).read_text())
        change(receipt)
        (folder / name).write_text(json.dumps(receipt))
    else:
        rewrite(folder, name, change)
    with pytest.raises(ValueError, match=f"^{message}$"):
        dataset_replay.replay(folder)


@pytest.mark.parametrize("name", ["inputs.json", "manifest.json", "dataset.csv"])
def test_replay_detects_a_changed_member_without_a_new_receipt(sources, tmp_path, name):
    folder = bundle_folder(tmp_path)
    (folder / name).write_bytes((folder / name).read_bytes() + b" ")
    with pytest.raises(ValueError, match=f"^checksum mismatch: {name}$"):
        dataset_replay.replay(folder)


def test_replay_size_bound_is_inclusive(sources, tmp_path, monkeypatch):
    folder = bundle_folder(tmp_path)
    largest = max(
        (folder / n).stat().st_size for n in ("inputs.json", "manifest.json", "dataset.csv")
    )
    monkeypatch.setattr(dataset_replay, "MAX_BYTES", largest)
    dataset_replay.replay(folder)
    monkeypatch.setattr(dataset_replay, "MAX_BYTES", largest - 1)
    with pytest.raises(ValueError, match="^bundle member exceeds size bound$"):
        dataset_replay.replay(folder)


def test_replay_accepts_eight_series_and_rejects_duplicate_columns(sources, tmp_path):
    names = ["ipca", "rates", "zero", "nulls"]
    spec = {
        "snapshot_id": "s1",
        "series": [
            {"series_id": n, "operation": op} for n in names for op in ("level", "difference")
        ],
    }
    folder = bundle_folder(tmp_path / "eight", spec)
    assert dataset_replay.replay(folder) == (folder / "dataset.csv").read_bytes()
    folder = bundle_folder(tmp_path / "dup")

    def duplicate(manifest):
        manifest["specification"]["series"][1] = dict(manifest["specification"]["series"][0])

    rewrite(folder, "manifest.json", duplicate)
    rewrite(folder, "inputs.json", lambda inputs: inputs.__setitem__(1, inputs[0]))
    with pytest.raises(ValueError, match="^duplicate output column$"):
        dataset_replay.replay(folder)


def test_replay_date_bounds_are_inclusive(sources, tmp_path):
    spec = {**SPEC, "date_from": "2024-02-01", "date_to": "2024-05-01"}
    folder = bundle_folder(tmp_path, spec)
    assert dataset_replay.replay(folder) == (folder / "dataset.csv").read_bytes()


def test_replay_percent_change_and_index_handle_zero_bases(sources, tmp_path):
    spec = {
        "snapshot_id": "s1",
        "series": [
            {"series_id": "zero", "operation": "percent_change"},
            {"series_id": "zero", "operation": "index_100"},
            {"series_id": "rates", "operation": "percent_change"},
            {"series_id": "nulls", "operation": "index_100"},
            {"series_id": "rates", "operation": "rolling_mean", "window": 2},
            {"series_id": "ipca", "operation": "difference", "lag": 3},
        ],
    }
    folder = bundle_folder(tmp_path, spec)
    assert dataset_replay.replay(folder) == (folder / "dataset.csv").read_bytes()


# Release differences

PROVENANCE = {"record_id", "import_id", "source_hash", "source_line"}


def quote(close="30.00", **extra):
    return {
        "date": date(2026, 1, 1),
        "symbol": "TEST3",
        "tpmerc": "010",
        "codbdi": "02",
        "specification": "ON",
        "isin": "BRTESTACNOR1",
        "distribution": "100",
        "forward_term": "",
        "expiry_raw": "99991231",
        "correction_indicator": "0",
        "currency": "BRL",
        "quotation_factor": 1,
        "exercise_price_scaled": 0,
        "exercise_points_scaled": 0,
        "close": close,
        "record_id": "r",
        "import_id": "i",
        "source_hash": "h",
        "source_line": 2,
        **extra,
    }


def diff(monkeypatch, left_rows, right_rows, release=lambda r: r):
    calls = []

    def fake(db, **kwargs):
        calls.append((db, kwargs))
        rows = left_rows if kwargs["release"] == "a" else right_rows
        return {"release": release(kwargs["release"]), "rows": rows}

    monkeypatch.setattr(query, "quote_query", fake)
    result = release_diff(
        "db", left="a", right="b", symbol="TEST3", market="010", date_from="d1", date_to="d2"
    )
    return result, calls


def test_release_diff_lists_added_removed_and_changed_records(monkeypatch):
    kept = quote()
    changed_before, changed_after = (
        quote(date=date(2026, 1, 2)),
        quote(date=date(2026, 1, 2), close="31.00"),
    )
    removed, added = quote(date=date(2026, 1, 3)), quote(date=date(2026, 1, 4))
    provenance = {**kept, "record_id": "x", "import_id": "y", "source_hash": "z", "source_line": 9}
    result, calls = diff(
        monkeypatch,
        [kept, changed_before, removed],
        [provenance, changed_after, {**added, "extra": None}],
    )
    assert [c[1]["release"] for c in calls] == ["a", "b"]
    assert all(c[0] == "db" for c in calls)
    assert calls[0][1] == {
        "release": "a",
        "symbol": "TEST3",
        "market": "010",
        "date_from": "d1",
        "date_to": "d2",
        "limit": 10001,
    }

    def identity(row):
        from ofl.research.cotahist import IDENTITY

        return [str(row.get(k)) for k in IDENTITY]

    assert result == {
        "left_release": "a",
        "right_release": "b",
        "scope": {"symbol": "TEST3", "market": "010", "date_from": "d1", "date_to": "d2"},
        "changes": [
            {
                "identity": identity(changed_before),
                "state": "changed",
                "fields": ["close"],
                "before": changed_before,
                "after": changed_after,
            },
            {
                "identity": identity(removed),
                "state": "removed",
                "fields": sorted(set(removed) - PROVENANCE),
                "before": removed,
                "after": None,
            },
            {
                "identity": identity(added),
                "state": "added",
                "fields": sorted(set(added) - PROVENANCE),
                "before": None,
                "after": {**added, "extra": None},
            },
        ],
        "qualification": "Record-level observed release differences within explicit scope; "
        "not a corporate-action continuity proof.",
    }


def test_release_diff_reports_a_field_present_on_one_side_only(monkeypatch):
    result, _ = diff(monkeypatch, [quote()], [{**quote(), "adjusted": "1"}])
    assert result["changes"][0]["fields"] == ["adjusted"]
    result, _ = diff(monkeypatch, [{**quote(), "adjusted": "1"}], [quote()])
    assert result["changes"][0]["fields"] == ["adjusted"]


def test_release_diff_accepts_10000_rows_per_release(monkeypatch):
    rows = [quote(date=date.fromordinal(700000 + i)) for i in range(10000)]
    assert diff(monkeypatch, rows, rows)[0]["changes"] == []


@pytest.mark.parametrize(
    "left,right,release",
    [
        ([quote()], [quote()], lambda r: None if r == "b" else r),
        ([quote(date=date.fromordinal(700000 + i)) for i in range(10001)], [], lambda r: r),
        ([], [quote(date=date.fromordinal(700000 + i)) for i in range(10001)], lambda r: r),
    ],
)
def test_release_diff_needs_both_releases_within_bounds(monkeypatch, left, right, release):
    with pytest.raises(ValueError, match="^release unavailable or diff exceeds 10,000 rows$"):
        diff(monkeypatch, left, right, release)


def test_release_diff_refuses_ambiguous_records(monkeypatch):
    with pytest.raises(ValueError, match="^ambiguous records within a release$"):
        diff(monkeypatch, [quote(), quote(close="31.00")], [])


def test_release_diff_reports_a_record_added_without_economic_values(monkeypatch):
    bare = {"record_id": "r", "source_line": 3}
    result, _ = diff(monkeypatch, [], [bare])
    assert [(c["state"], c["fields"], c["after"]) for c in result["changes"]] == [
        ("added", [], bare)
    ]


def test_bundle_reads_its_sources_through_the_given_database_and_store(sources):
    dataset_bundle.bundle("db", "store", SPEC)
    assert {call[:2] for call in sources} == {("db", "store")}


def test_series_without_any_value_has_no_index_and_still_replays(sources, tmp_path):
    spec = {"snapshot_id": "s1", "series": [{"series_id": "empty", "operation": "index_100"}]}
    assert table(prepare_dataset("db", "store", spec)["csv"]) == [
        ["date", "empty__index_100"],
        ["2024-01-01", ""],
        ["2024-02-01", ""],
    ]
    folder = bundle_folder(tmp_path, spec)
    assert dataset_replay.replay(folder) == (folder / "dataset.csv").read_bytes()


def test_replay_accepts_one_series_and_refuses_more_than_eight(sources, tmp_path):
    single = {"snapshot_id": "s1", "series": [{"series_id": "ipca"}]}
    folder = bundle_folder(tmp_path / "one", single)
    assert dataset_replay.replay(folder) == (folder / "dataset.csv").read_bytes()
    folder = bundle_folder(tmp_path / "nine", single)
    names = [f"s{i}" for i in range(9)]
    rewrite(
        folder,
        "manifest.json",
        recipe(lambda s: s.update(series=[{**s["series"][0], "series_id": n} for n in names])),
    )

    def nine(inputs):
        inputs[:] = [{**inputs[0], "series": {**inputs[0]["series"], "key": n}} for n in names]

    rewrite(folder, "inputs.json", nine)
    with pytest.raises(ValueError, match="^input/recipe series mismatch$"):
        dataset_replay.replay(folder)
