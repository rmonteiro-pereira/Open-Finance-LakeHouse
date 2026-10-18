"""Do not invent relevance, abstention or measured denominators from missing reviews."""

import copy

import pytest

from ofl.research.db import digest
from ofl.research.retrieval_review import MODES, assess, worksheet


def fixture():
    candidates = []
    for identifier in ("one", "two"):
        candidate = {
            "item_id": identifier,
            "passages": [{"id": identifier, "text": "Frozen source"}],
        }
        candidate["capture_sha256"] = digest(candidate)
        candidates.append(candidate)
    pool = {
        "version": 1,
        "cases": [
            {
                "id": "R1",
                "query": "inflação Brasil",
                "filters": {},
                "candidates": candidates,
                "rankings": {
                    "keyword": {"status": "ok", "item_ids": ["one"]},
                    "vector": {"status": "ok", "item_ids": ["two", "one"]},
                    "hybrid": {"status": "error", "item_ids": []},
                },
            }
        ],
    }
    review = worksheet(pool)
    review["reviewer"] = "builder; not independent"
    return pool, review


def test_missing_reviews_are_not_negatives_and_errors_are_not_empty_successes():
    pool, review = fixture()
    report = assess(pool, review)
    assert "rankings" not in review["cases"][0]
    for mode in MODES:
        assert report["summary"][mode]["precision_returned_at_10"] == {
            "mean": None,
            "assessed_cases": 0,
        }
        assert report["cases"][0]["modes"][mode]["correct_abstention"] is None
    assert report["summary"]["hybrid"]["errors"] == 1
    assert report["cases"][0]["modes"]["hybrid"]["empty_result"] is None


def test_partial_review_has_only_supported_metrics():
    pool, review = fixture()
    review["cases"][0]["candidates"][0].update(
        verdict="relevant", rationale="Brazilian inflation context"
    )
    report = assess(pool, review)
    assert report["summary"]["keyword"]["precision_returned_at_10"]["mean"] == 1
    assert report["summary"]["vector"]["precision_returned_at_10"]["mean"] is None
    assert report["summary"]["keyword"]["pooled_recall_at_10"]["mean"] is None
    review["cases"][0]["candidates"][1].update(verdict="uncertain", rationale="Geography unclear")
    assert assess(pool, review)["cases"][0]["uncertain_candidates"] == 1
    review["cases"][0]["candidates"][1].update(verdict="irrelevant", rationale="US context only")
    complete = assess(pool, review)
    assert complete["summary"]["vector"]["precision_returned_at_10"]["mean"] == 1 / 2
    assert complete["summary"]["vector"]["pooled_recall_at_10"]["mean"] == 1
    assert complete["reviewer_independence"] == "not_verified"


@pytest.mark.parametrize(
    "mutation", ["pool", "source", "question", "duplicate", "rationale", "reviewer"]
)
def test_rejects_unbound_or_unjustified_reviews(mutation):
    pool, review = fixture()
    if mutation == "pool":
        pool["cases"][0]["query"] = "changed"
    elif mutation == "source":
        review["cases"][0]["candidates"][0]["passages"][0]["text"] = "changed"
    elif mutation == "question":
        review["cases"][0]["query"] = "changed"
    elif mutation == "duplicate":
        review["cases"][0]["candidates"] = [copy.deepcopy(review["cases"][0]["candidates"][0])] * 2
    elif mutation == "rationale":
        review["cases"][0]["candidates"][0]["verdict"] = "relevant"
    else:
        review["reviewer"] = ""
    with pytest.raises(ValueError):
        assess(pool, review)


def test_empty_pool_is_not_proof_of_correct_abstention():
    pool, _ = fixture()
    pool["cases"][0]["candidates"] = []
    pool["cases"][0]["rankings"] = {m: {"status": "ok", "item_ids": []} for m in MODES}
    review = worksheet(pool)
    review["reviewer"] = "builder"
    report = assess(pool, review)
    assert report["cases"][0]["modes"]["keyword"]["empty_result"] is True
    assert report["summary"]["keyword"]["pooled_recall_at_10"]["mean"] is None
