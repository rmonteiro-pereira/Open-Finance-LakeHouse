"""End-to-end product contracts; fixtures do not establish customer outcomes."""

import hashlib
import json
from datetime import date
from decimal import Decimal

import pytest

pytest.importorskip("psycopg")
from fastapi.testclient import TestClient
from ofl.research.api import create_app
from ofl.research.analytics import portfolio, prepare_dataset
from ofl.research.library import sync_library
from ofl.research.macro import sync_macro
from ofl.research.search import search
from ofl.research.workspaces import load, save, rerun, schedule_due
from tests.test_research import research  # noqa: F401
from tests.test_workbench import LibraryFixture, news, snapshot


def seed(research):
    db, store = research
    source = LibraryFixture({"news/items/official-source/a.jsonl": json.dumps(news()).encode()})
    sync_library(db, store, "news", source=source)
    return search(db, "inflação")["results"][0]


def test_owned_immutable_research_and_optimistic_conflict(research):
    db, store = research
    hit = seed(research)
    body = {
        "question": "O que sabemos sobre inflação?",
        "references": [hit["id"]],
        "findings": [
            {
                "kind": "observation",
                "text": "Indicadores documentados e contexto econômico.",
                "references": [hit["id"]],
            }
        ],
    }
    first = save(db, "owner", "investigation", "Inflação", body)
    record = load(db, "owner", first["id"])
    assert record["body"]["content"]["evidence"][0]["asset_id"] == hit["asset_id"]
    assert record["body"]["content"]["findings"][0]["status"] == "source_excerpt"
    second = save(
        db,
        "owner",
        "investigation",
        "Inflação atualizada",
        {**body, "limitations": "Leitura parcial"},
        workspace_id=first["id"],
        expected_revision=1,
    )
    assert second["revision"] == 2
    assert load(db, "owner", first["id"], 1)["sha256"] == first["sha256"]
    with pytest.raises(LookupError):
        load(db, "reader", first["id"])
    with pytest.raises(FileExistsError):
        save(
            db,
            "owner",
            "investigation",
            "stale",
            body,
            workspace_id=first["id"],
            expected_revision=1,
        )
    with pytest.raises(ValueError, match="quote"):
        save(
            db,
            "owner",
            "investigation",
            "fabrication",
            {
                **body,
                "findings": [
                    {
                        "kind": "observation",
                        "text": "Unfounded performance of 99%",
                        "references": [hit["id"]],
                    }
                ],
            },
        )


def test_current_policy_redacts_saved_evidence_and_linked_findings(research):
    db, store = research
    hit = seed(research)
    saved = save(
        db,
        "owner",
        "investigation",
        "Tema",
        {
            "question": "Inflação?",
            "references": [hit["id"]],
            "findings": [
                {
                    "kind": "observation",
                    "text": "Indicadores documentados e contexto econômico.",
                    "references": [hit["id"]],
                }
            ],
        },
    )
    src = LibraryFixture({"news/items/official-source/a.jsonl": json.dumps(news()).encode()})
    src.news_tiers = {"official-source": "headline_only"}
    sync_library(db, store, "news", source=src)
    record = load(db, "owner", saved["id"])
    assert hit["id"] in record["redacted_references"]
    assert "Indicadores documentados" not in json.dumps(record["body"])
    assert record["sha256"] == saved["sha256"]


def test_dataset_transform_nulls_and_hash(research):
    db, store = research
    sync_macro(
        db,
        store,
        source=snapshot(
            values=[
                {"series_id": "ipca", "date": date(2026, 1, 1), "value": 1.0},
                {"series_id": "ipca", "date": date(2026, 2, 1), "value": 2.0},
            ]
        ),
    )
    data = prepare_dataset(
        db,
        store,
        {"snapshot_id": "s1", "series": [{"series_id": "ipca", "operation": "percent_change"}]},
    )
    assert data["preview"][0]["ipca__percent_change"] is None
    assert Decimal(data["preview"][1]["ipca__percent_change"]) == 100
    assert data["manifest"]["csv_sha256"] == hashlib.sha256(data["csv"].encode()).hexdigest()
    assert data["manifest"]["specification"]["snapshot_id"] == "s1"


@pytest.mark.parametrize(
    "operation,values,expected",
    [
        ("difference", [1.0, 2.0], "1.0"),
        ("percent_change", [0.0, 2.0], None),
        ("index_100", [0.0, 2.0], None),
        ("rolling_mean", [1.0, 2.0], "1.5"),
    ],
)
def test_transform_edges(research, operation, values, expected):
    db, store = research
    sync_macro(
        db,
        store,
        source=snapshot(
            values=[
                {"series_id": "ipca", "date": date(2026, i + 1, 1), "value": v}
                for i, v in enumerate(values)
            ]
        ),
    )
    result = prepare_dataset(
        db,
        store,
        {
            "snapshot_id": "s1",
            "series": [{"series_id": "ipca", "operation": operation, "window": 2}],
        },
    )
    assert result["preview"][1]["ipca__" + operation] == expected


def valued():
    return {
        "currency": "BRL",
        "valuation_date": "2026-10-18",
        "positions": [
            {
                "instrument_id": "stable-1",
                "market": "010",
                "currency": "BRL",
                "asset_class": "equity",
                "market_value": "9007199254740993.12",
            },
            {
                "instrument_id": "stable-2",
                "market": "account",
                "currency": "BRL",
                "asset_class": "cash",
                "market_value": "6.88",
            },
        ],
        "scenarios": [{"name": "Stress", "shocks": {"equity": "-10", "cash": "0"}}],
    }


def test_portfolio_exact_values_and_explicit_scenario():
    result = portfolio(valued())
    assert result["total_value"] == "9007199254741000.00"
    assert Decimal(result["scenarios"][0]["value_change"]) == Decimal("-900719925474099.312")
    assert result["input_sha256"]


@pytest.mark.parametrize(
    "failure", ["currency", "nan", "negative", "duplicates", "missing_shock", "float"]
)
def test_portfolio_rejects_unsupported_assumptions(failure):
    body = valued()
    if failure == "currency":
        body["positions"][0]["currency"] = "USD"
    if failure == "nan":
        body["positions"][0]["market_value"] = "NaN"
    if failure == "negative":
        body["positions"][0]["market_value"] = "-1"
    if failure == "duplicates":
        body["positions"].append(body["positions"][0])
    if failure == "missing_shock":
        del body["scenarios"][0]["shocks"]["cash"]
    if failure == "float":
        body["positions"][0]["market_value"] = 1.2
    with pytest.raises(ValueError):
        portfolio(body)


def test_saved_checks_repeat_immutably_and_schedule_once(research):
    db, store = research
    seed(research)
    saved = save(
        db,
        "owner",
        "check",
        "Inflação",
        {"type": "search", "q": "inflação", "result": {"forged": True}},
    )
    assert "result" not in load(db, "owner", saved["id"])["body"]["content"]
    ran = rerun(db, store, "owner", saved["id"])
    again = rerun(db, store, "owner", saved["id"])
    assert ran["revision"] == 2 and again["revision"] == 3
    body = load(db, "owner", saved["id"])["body"]["content"]
    assert body["comparison"]["changed"] is False
    with db.connect() as con:
        con.execute(
            "INSERT INTO check_schedules(workspace_id,owner,interval_minutes) VALUES (%s,%s,15)",
            (saved["id"], "owner"),
        )
    assert schedule_due(db) == 1 and schedule_due(db) == 0
    assert len(db.rows("SELECT * FROM jobs WHERE kind='research_check'")) == 1


def test_api_ownership_exports_feedback_and_roles(research):
    db, store = research
    api = TestClient(create_app(db.settings))
    headers = {
        "X-OFL-Gateway-Token": "test-gateway",
        "X-OFL-Identity": "owner",
        "Origin": db.settings.origin,
    }
    created = api.post(
        "/api/v1/workspaces",
        headers=headers,
        json={"kind": "portfolio", "title": "Carteira", "content": valued()},
    )
    assert created.status_code == 201, created.text
    key = created.json()["id"]
    reader = {**headers, "X-OFL-Identity": "reader"}
    assert api.get("/api/v1/workspaces/" + key, headers=reader).status_code == 404
    assert (
        api.get(
            "/api/v1/workspaces/" + key + "/export?revision=1&format=markdown", headers=headers
        ).status_code
        == 200
    )
    fb = api.post(
        "/api/v1/feedback",
        headers=reader,
        json={"category": "coverage", "detail": "Missing date coverage"},
    ).json()
    assert (
        api.post(
            "/api/v1/feedback/" + fb["id"] + "/review",
            headers=reader,
            json={"state": "triaged", "note": "x"},
        ).status_code
        == 403
    )
    response = api.post(
        "/api/v1/feedback/" + fb["id"] + "/review",
        headers=headers,
        json={"state": "evaluation_candidate", "note": "Reproduce on held-out dates"},
    )
    assert response.status_code == 200, response.text
    assert (
        api.get("/api/v1/feedback/" + fb["id"] + "/history", headers=reader).json()[0]["revision"]
        == 1
    )
    assert (
        api.post(
            "/api/v1/workspaces",
            headers={**headers, "Origin": "https://evil.test"},
            json={"kind": "portfolio", "title": "no", "content": valued()},
        ).status_code
        == 403
    )


def test_watchlist_rejects_ticker_only_and_job_events(research):
    db, _ = research
    with pytest.raises(ValueError):
        save(db, "owner", "watchlist", "ticker", {"entries": [{"symbol": "PETR4"}]})
    saved = save(
        db,
        "owner",
        "watchlist",
        "identity",
        {
            "entries": [
                {"isin": "BRPETRACNPR6", "market": "010", "currency": "R$", "distribution": "100"}
            ]
        },
    )
    assert saved["revision"] == 1
    job = db.enqueue("test", {})
    claim = db.claim("worker")
    assert claim["id"] == job
    db.finish(job, "worker")
    assert [
        r["state"]
        for r in db.rows("SELECT state FROM job_events WHERE job_id=%s ORDER BY id", (job,))
    ] == ["queued", "running", "done"]


def test_exact_comparison_rejects_contract_changes(monkeypatch):
    from ofl.research.analytics import compare_quotes
    from ofl.research import query

    rows = [
        {
            "date": date(2026, 1, 1),
            "isin": "ISIN",
            "symbol": "TEST",
            "tpmerc": "010",
            "currency": "BRL",
            "distribution": "100",
            "quotation_factor": 1,
            "close_per_unit": Decimal("9007199254740993.12"),
        },
        {
            "date": date(2026, 1, 2),
            "isin": "ISIN",
            "symbol": "TEST",
            "tpmerc": "010",
            "currency": "BRL",
            "distribution": "100",
            "quotation_factor": 1,
            "close_per_unit": Decimal("9007199254740994.12"),
        },
    ]
    monkeypatch.setattr(query, "quote_query", lambda *a, **kw: {"release": "r1", "rows": rows})
    result = compare_quotes(
        None,
        symbol="TEST",
        market="010",
        date_from="2026-01-01",
        date_to="2026-01-02",
        release="r1",
    )
    assert result["difference_per_unit"] == "1.00"
    rows[1]["exercise_points_scaled"] = 2
    with pytest.raises(ValueError, match="identity"):
        compare_quotes(
            None,
            symbol="TEST",
            market="010",
            date_from="2026-01-01",
            date_to="2026-01-02",
            release="r1",
        )


def test_release_diff_ignores_only_provenance_and_detects_actual_change(monkeypatch):
    from ofl.research.analytics import release_diff
    from ofl.research import query

    base = {
        "date": date(2026, 1, 1),
        "isin": "ISIN",
        "distribution": "100",
        "tpmerc": "010",
        "symbol": "TEST",
        "currency": "BRL",
        "quotation_factor": 1,
        "close": "30.00",
        "record_id": "a",
        "source_hash": "x",
        "source_line": 2,
    }
    revised = {**base, "source_hash": "y", "source_line": 4}
    monkeypatch.setattr(
        query,
        "quote_query",
        lambda *a, **kw: {
            "release": kw["release"],
            "rows": [base if kw["release"] == "a" else revised],
        },
    )
    args = {
        "left": "a",
        "right": "b",
        "symbol": "TEST",
        "market": "010",
        "date_from": "2026-01-01",
        "date_to": "2026-01-02",
    }
    assert release_diff(None, **args)["changes"] == []
    revised["close"] = "31.00"
    result = release_diff(None, **args)
    assert result["changes"][0]["fields"] == ["close"]


def test_coverage_check_does_not_alert_on_its_own_job_count(research):
    db, store = research
    saved = save(db, "owner", "check", "Coverage", {"type": "coverage"})
    rerun(db, store, "owner", saved["id"])
    db.enqueue("research_check", {"workspace_id": saved["id"], "owner": "owner"})
    rerun(db, store, "owner", saved["id"])
    result = load(db, "owner", saved["id"])["body"]["content"]
    assert result["comparison"]["changed"] is False
    assert result["result"]["jobs"]


def test_difference_of_percentage_series_is_labeled_percentage_points(research):
    db, store = research
    sync_macro(db, store, source=snapshot())
    result = prepare_dataset(
        db,
        store,
        {"snapshot_id": "s1", "series": [{"series_id": "ipca", "operation": "difference"}]},
    )
    assert result["manifest"]["columns"][0]["output_unit"] == "percentage_points"
    assert result["manifest"]["columns"][0]["source"]["unit"] == "%"
