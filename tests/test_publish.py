"""The sink's refusal, and the watchdog's verdict — both as tables of stated cases.

The watchdog is the piece whose whole value is behaving correctly when the producer is
dead, which is the one condition that cannot be observed by running the producer. Making
`health` a pure function of (latest.json, now) is what turns "40 hours old reads red" into
an assertion instead of a day-and-a-half wait.
"""

import datetime as dt
import json
from pathlib import Path

import pytest

from ofl.release.publish import PublishRefused, health, latest_pointer, publish

NOW = dt.datetime(2026, 8, 6, 12, 0, tzinfo=dt.timezone.utc)


def _latest(hours_ago: float) -> dict:
    return {
        "release_id": "2026-08-05.1",
        "published_at": (NOW - dt.timedelta(hours=hours_ago)).isoformat(),
    }


@pytest.mark.parametrize(
    ("hours", "state", "reason"),
    [
        (2, "green", "ok"),
        (25, "green", "ok"),
        (26, "amber", "build_ageing"),
        (35, "amber", "build_ageing"),
        (36, "red", "build_stale"),
        (40, "red", "build_stale"),
        (240, "red", "build_stale"),
    ],
)
def test_health_verdicts_are_a_written_table(hours, state, reason):
    out = health(_latest(hours), NOW)
    assert (out["state"], out["reason"]) == (state, reason)
    assert out["build_age_hours"] == pytest.approx(hours, abs=0.01)


def test_a_missing_latest_is_a_different_red_from_a_stale_one():
    """"The producer never published" and "the producer stopped" call for different
    responses, so they must not arrive under the same label."""
    missing = health(None, NOW)
    stale = health(_latest(40), NOW)
    assert missing["state"] == stale["state"] == "red"
    assert missing["reason"] != stale["reason"]
    assert missing["reason"] == "no_latest_json"


def test_a_malformed_timestamp_is_red_not_an_exception():
    """A watchdog that crashes on bad input is a watchdog that reports nothing on exactly
    the day something is wrong."""
    out = health({"release_id": "x", "published_at": "not-a-time"}, NOW)
    assert out["state"] == "red"
    assert out["reason"] == "unparseable_published_at"


def test_health_names_its_author():
    """`health.json` must be distinguishable from anything the producer wrote — its whole
    purpose is to be a second opinion."""
    assert health(_latest(2), NOW)["checked_by"] == "watchdog"


def test_latest_is_a_pointer_with_a_hash_not_a_directory_alias():
    manifest = {"release_id": "2026-08-05.1", "generated_at": NOW.isoformat()}
    ptr = latest_pointer(manifest, manifest_sha256="abc123", base_url="https://example/dl")
    assert set(ptr) == {"release_id", "manifest_url", "manifest_sha256", "published_at"}
    # Four fields answer "did the target move?" without downloading a release.
    assert ptr["manifest_sha256"] == "abc123"


def _write_manifest(tmp_path: Path, **over) -> Path:
    d = tmp_path / "rel"
    d.mkdir()
    manifest = {
        "release_id": "1970-01-01.1",
        "release_class": "fixture",
        "publishable": False,
        "generated_at": NOW.isoformat(),
    }
    manifest.update(over)
    (d / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (d / "parquet").mkdir()
    (d / "parquet" / "t.parquet").write_bytes(b"x")
    return d


def test_the_sink_refuses_a_fixture_release_by_construction(tmp_path):
    """The check lives in the sink, not in the caller who might forget it. Publishing a
    fixture would put synthetic data behind a public URL."""
    d = _write_manifest(tmp_path)
    with pytest.raises(PublishRefused, match="publishable=False"):
        publish(d, sink="gh://rmonteiro-pereira/ofl-public-data")


def test_a_production_release_produces_a_plan_offline(tmp_path):
    d = _write_manifest(tmp_path, release_class="production", publishable=True)
    plan = publish(d, sink="gh://rmonteiro-pereira/ofl-public-data", dry_run=True)
    assert plan["tag"] == "data-1970-01-01.1"
    assert "manifest.json" in plan["assets"]
    assert plan["repo"] == "rmonteiro-pereira/ofl-public-data"


def test_live_publishing_refuses_without_the_deployment_step(tmp_path):
    """Honest failure: the token and the data repository are a human step, and pretending
    otherwise would ship an untested code path as if it worked."""
    d = _write_manifest(tmp_path, release_class="production", publishable=True)
    with pytest.raises(PublishRefused, match="docs/DEPLOY.md"):
        publish(d, sink="gh://rmonteiro-pereira/ofl-public-data", dry_run=False)


def test_an_unknown_sink_is_refused(tmp_path):
    d = _write_manifest(tmp_path, release_class="production", publishable=True)
    with pytest.raises(PublishRefused, match="unknown sink"):
        publish(d, sink="s3://bucket")
