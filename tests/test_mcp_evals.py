"""The evals as a gate: they have to be able to go red, and they have to say so.

"Prints a scoreboard including the failures" is satisfied by a suite with no failures.
What makes this a gate is the committed floor plus a non-zero exit below it, and the two
are tested by breaking a case on purpose.
"""

from pathlib import Path

import pytest

from ofl.mcp.evals import context_from_corpus, load_cases, run_case, run_evals, threshold

CORPUS = Path(__file__).parent / "fixtures" / "release"


@pytest.fixture
def ctx():
    """The SAME context builder the CLI uses. Two builders meant two corpora, and a gold
    case that passed in one and failed in the other was measuring the fixture."""
    return context_from_corpus(CORPUS)


def test_the_cases_are_versioned_and_cover_refusals(ctx):
    cases = load_cases()
    assert len(cases) >= 12
    refusals = [c for c in cases if c.get("expect_refusal")]
    # Refusing correctly is the behaviour that keeps a macro agent from confidently
    # returning a wrong number, so it cannot be a minority of the suite.
    assert len(refusals) >= len(cases) // 3


def test_the_suite_passes_against_the_frozen_corpus(ctx):
    report = run_evals(ctx)
    assert report["failures"] == []
    assert report["ok"] is True
    assert report["pass_rate"] == 1.0


def test_the_floor_is_committed(ctx):
    assert 0 < threshold() <= 1.0


def test_a_broken_case_turns_the_suite_red(ctx):
    """The falsifiability check. A suite that cannot go red is not measuring anything."""
    cases = load_cases()
    cases.append(
        {
            "id": "deliberately-wrong",
            "tool": "describe_series",
            "args": {"series_id": "selic"},
            "expect": {"basis": "per_year"},  # it is per_day
        }
    )
    report = run_evals(ctx, cases)
    assert report["ok"] is False
    assert report["pass_rate"] < 1.0
    assert any(f["id"] == "deliberately-wrong" for f in report["failures"])


def test_an_answer_where_a_refusal_was_specified_is_a_failure(ctx):
    """Otherwise a regression that removes a refusal reads as a pass."""
    result = run_case(
        ctx,
        {"id": "x", "tool": "describe_series", "args": {"series_id": "selic"},
         "expect_refusal": "not_redistributable"},
    )
    assert result.passed is False
    assert "expected refusal" in result.detail


def test_a_refusal_with_the_wrong_code_is_a_failure(ctx):
    """Refusing for the wrong reason is not the same as refusing."""
    result = run_case(
        ctx,
        {"id": "x", "tool": "describe_series", "args": {"series_id": "anbima_ima_b"},
         "expect_refusal": "unknown_series"},
    )
    assert result.passed is False
    assert "wanted" in result.detail


def test_failures_are_enumerated_not_counted(ctx):
    cases = [{"id": "boom", "tool": "describe_series", "args": {"series_id": "selic"},
              "expect": {"unit": "brl"}}]
    report = run_evals(ctx, cases)
    assert report["failures"][0]["id"] == "boom"
    assert "wanted" in report["failures"][0]["detail"]
