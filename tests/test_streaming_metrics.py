"""M3/M4: the AvailableNow trigger, the metrics snapshot, and the cron workflow.

Everything here runs without a JVM. The parts that genuinely need Spark — that
``Trigger.AvailableNow`` terminates, that a second run writes nothing — are
measured by ``tools/streaming_idempotence.py`` against real Delta tables, and the
transcript is committed under ``docs/evidence/streaming/``. What is *unit*-tested
is the arithmetic and the wiring around it: the throughput derivation, the
comparison that decides "idempotent or not", and the workflow's inertness.
"""

from pathlib import Path

import pytest
import yaml

from ofl.streaming.bronze import trigger_for
from ofl.streaming.metrics import (
    IDEMPOTENT_FIELDS,
    compare_states,
    run_metrics,
    table_metrics,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "streaming.yml"

# Shape of a real run summary, trimmed to the fields the metrics read. Values are
# from the committed AvailableNow run 1 (see docs/evidence/streaming/).
RUN_1 = {
    "batches": 2,
    "windows": 9,
    "mode": "availableNow",
    "dropped_late": 0,
    "watermark": "2026-07-29T04:29:09.190Z",
    "open_windows": ["04:29:00", "04:30:00", "04:31:00"],
    "progress": [
        {"batchId": 7, "numInputRows": 9325, "durationMs": 4853},
        {"batchId": 8, "numInputRows": 0, "durationMs": 4896},
    ],
}

# ...and of run 2, which read nothing at all.
RUN_2 = {
    "batches": 0,
    "windows": 0,
    "mode": "availableNow",
    "dropped_late": 0,
    "watermark": "2026-07-29T04:29:09.190Z",
    "open_windows": ["04:29:00", "04:30:00", "04:31:00"],
    "progress": [{"batchId": 9, "numInputRows": 0, "durationMs": 81}],
}

STATE = {
    "exists": True,
    "rows": 26,
    "distinct_keys": 26,
    "duplicate_keys": 0,
    "symbols": 3,
    "total_trades": 24199,
    "delta_version": 3,
}


# --- the trigger ------------------------------------------------------------


def test_available_now_and_processing_time_are_the_only_two_modes():
    assert trigger_for(available_now=True, interval="10 seconds") == {"availableNow": True}
    assert trigger_for(available_now=False, interval="10 seconds") == {
        "processingTime": "10 seconds"
    }


def test_available_now_ignores_the_interval():
    # It is not a "very short interval": there is no clock, the query drains and exits.
    fast = trigger_for(available_now=True, interval="1 second")
    slow = trigger_for(available_now=True, interval="1 hour")
    assert fast == slow


def test_the_trigger_dict_is_a_writestream_trigger_kwarg():
    # Guards the call site: `.trigger(**trigger_for(...))` must be a single kwarg,
    # because Spark rejects a trigger() call carrying two.
    for available_now in (True, False):
        assert len(trigger_for(available_now=available_now, interval="10 seconds")) == 1


# --- run metrics ------------------------------------------------------------


def test_throughput_is_rows_over_spark_s_own_trigger_time():
    m = run_metrics(RUN_1)
    assert m["input_rows"] == 9325
    assert m["trigger_ms"] == 4853 + 4896
    assert m["throughput_rows_per_second"] == pytest.approx(9325 / 9.749, rel=1e-3)


def test_a_run_that_read_nothing_reports_zero_rather_than_dividing_by_zero():
    m = run_metrics(RUN_2)
    assert (m["input_rows"], m["rows_written"], m["batches"]) == (0, 0, 0)
    assert m["throughput_rows_per_second"] == 0.0


def test_run_metrics_survive_a_summary_with_no_progress_at_all():
    # A query stopped before its first trigger has an empty recentProgress.
    assert run_metrics({"batches": 0, "windows": 0})["throughput_rows_per_second"] == 0.0


def test_the_watermark_and_the_held_windows_travel_into_the_snapshot():
    m = run_metrics(RUN_1)
    assert m["watermark"] == "2026-07-29T04:29:09.190Z"
    assert m["open_windows"] == ["04:29:00", "04:30:00", "04:31:00"]
    assert m["dropped_late"] == 0


def test_a_bronze_summary_reports_its_own_row_count():
    # bronze names the count `bronze_rows`, silver names it `windows`; one snapshot
    # shape has to read both without lying about which it got.
    assert run_metrics({"batches": 1, "bronze_rows": 15908})["rows_written"] == 15908


# --- the idempotence comparison ---------------------------------------------


def test_two_identical_states_compare_equal():
    assert compare_states(STATE, dict(STATE)) == {}


def test_a_row_that_appeared_between_runs_is_reported_with_both_values():
    drifted = {**STATE, "rows": 27, "distinct_keys": 27}
    assert compare_states(STATE, drifted) == {"rows": (26, 27), "distinct_keys": (26, 27)}


def test_a_republished_window_is_caught_even_though_the_row_count_moved_too():
    # The failure mode that matters: a window written twice. Both `rows` and
    # `duplicate_keys` move, and the report has to name the duplicate.
    republished = {**STATE, "rows": 27, "duplicate_keys": 1}
    assert "duplicate_keys" in compare_states(STATE, republished)


def test_the_comparison_ignores_what_a_second_run_is_allowed_to_change():
    # Run 2 legitimately has a different capture time; if it wrote nothing the Delta
    # version is unchanged, but folding version into the check would make a run that
    # *did* write correctly look like a failure. Neither belongs in the comparison.
    later = {**STATE, "captured_at": "later", "delta_version": 99}
    assert compare_states(STATE, later) == {}
    assert "delta_version" not in IDEMPOTENT_FIELDS


def test_a_missing_table_is_not_silently_equal_to_an_empty_one():
    absent = table_metrics(Path("does") / "not" / "exist")
    assert absent["exists"] is False
    assert compare_states(STATE, absent) != {}


# --- the cron workflow, which must stay inert -------------------------------


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_the_workflow_parses():
    assert _workflow()["name"] == "streaming-lane"


def test_the_workflow_cannot_fire_on_its_own():
    # `on:` parses as the boolean True in YAML 1.1 — that is the famous Norway
    # problem, and asserting on the wrong key would make this test vacuous.
    triggers = _workflow()[True]
    assert set(triggers) == {"workflow_dispatch"}
    assert "schedule" not in triggers  # the cron line stays a comment until R2 exists


def test_the_run_is_gated_on_the_r2_secrets():
    jobs = _workflow()["jobs"]
    assert jobs["stream"]["needs"] == "preflight"
    gate = jobs["preflight"]["steps"][0]
    for secret in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"):
        assert secret in gate["env"]
        assert secret in gate["run"]
    assert "exit 1" in gate["run"]


def test_both_spark_passes_run_under_available_now():
    # A scheduled job has to terminate; a processing-time trigger never would.
    steps = _workflow()["jobs"]["stream"]["steps"]
    spark_steps = [s for s in steps if "stream-bronze" in s.get("run", "") or "stream-silver" in s.get("run", "")]
    assert len(spark_steps) == 2
    assert all("--available-now" in s["run"] for s in spark_steps)


def test_concurrent_runs_are_refused_rather_than_queued_over_one_checkpoint():
    concurrency = _workflow()["concurrency"]
    assert concurrency["cancel-in-progress"] is False
