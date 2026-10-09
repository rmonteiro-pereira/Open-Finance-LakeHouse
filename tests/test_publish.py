"""Public snapshot: licence tiers decide what leaves, and the pointers only move on success."""

import hashlib
import json

import duckdb
import pytest

from ofl.publish import MART_INPUTS, LocalTarget, PublishError, mart_tier, publish
from ofl.registry import load_registry
from ofl.transform.gold.runner import MODELS


@pytest.fixture
def registry():
    return load_registry("sources/registry.yml")


@pytest.fixture
def con():
    """Three marts (open, derived, private) and a fact table with an open and a private series."""
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE mart_macro_dashboard AS SELECT * FROM (VALUES "
        "(DATE '2024-01-01', 11.75, 0.42), (DATE '2024-02-01', 11.25, 0.83)) t(month, selic_target, ipca_mom)"
    )
    con.execute(
        "CREATE TABLE mart_di_curve_slope AS SELECT * FROM (VALUES "
        "(DATE '2024-02-01', 0.35, 'normal')) t(trade_date, slope_2s10s_pp, curve_shape)"
    )
    con.execute(
        "CREATE TABLE mart_equity_daily AS SELECT 'PETR4' AS symbol, DATE '2024-02-01' AS date, 38.1 AS close"
    )
    con.execute("CREATE TABLE mart_fx (series_id VARCHAR, date DATE, rate DOUBLE)")
    con.execute(
        "CREATE TABLE fact_observation AS SELECT * FROM (VALUES "
        "('selic', DATE '2024-01-02', 0.043), ('selic', DATE '2024-01-03', 0.043), "
        "('anbima_ima_b', DATE '2024-01-02', 9000.0)) t(series_id, date, value)"
    )
    yield con
    con.close()


def _publish(con, tmp_path, registry, **kwargs):
    return publish(
        con,
        LocalTarget(tmp_path),
        registry=registry,
        relation=lambda _layer, table: table,
        **kwargs,
    )


def test_every_mart_declares_inputs_known_to_the_registry(registry):
    assert set(MART_INPUTS) == set(MODELS)
    for mart, inputs in MART_INPUTS.items():
        assert set(inputs) <= set(registry.series), mart


def test_tiers_come_from_the_source_and_the_most_restrictive_wins(registry):
    assert registry.series["selic"].redistribution == "open"
    assert registry.series["b3_oi"].redistribution == "derived"
    assert registry.series["yahoo_global"].redistribution == "private"
    assert mart_tier("mart_macro_dashboard", registry) == "open"
    assert mart_tier("mart_di_curve_slope", registry) == "derived"
    # One Yahoo input makes the whole equity mart private; ANBIMA's sandbox does the same to the curve.
    assert mart_tier("mart_equity_daily", registry) == "private"
    assert mart_tier("mart_yield_curve", registry) == "private"
    assert mart_tier("mart_not_declared", registry) == "private"


def test_snapshot_holds_only_what_may_be_shown(con, tmp_path, registry):
    manifest = _publish(con, tmp_path, registry, run_id="r1")

    published = {m["name"]: m for m in manifest["marts"]}
    assert set(published) == {"mart_macro_dashboard", "mart_di_curve_slope"}
    assert published["mart_macro_dashboard"]["download"] is True
    assert published["mart_di_curve_slope"]["download"] is False
    assert (
        published["mart_macro_dashboard"]
        | {"rows": 2, "min_date": "2024-01-01", "max_date": "2024-02-01"}
        == (published["mart_macro_dashboard"])
    )

    reasons = {s["name"]: s["reason"] for s in manifest["skipped"]}
    assert reasons["mart_equity_daily"] == "private"
    assert reasons["mart_fx"] == "empty"
    assert reasons["mart_real_interest"] == "missing"
    assert not (tmp_path / "snapshots/r1/marts/mart_equity_daily.parquet").exists()

    for mart in manifest["marts"]:
        body = (tmp_path / mart["path"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == mart["sha256"]
        assert len(body) == mart["bytes"]

    observations = tmp_path / manifest["observations"]["path"]
    series = duckdb.sql(
        f"SELECT DISTINCT series_id FROM read_parquet('{observations.as_posix()}')"
    ).fetchall()
    assert series == [("selic",)]

    catalog = {
        s["key"]: s for s in json.loads((tmp_path / manifest["catalog"]).read_text())["series"]
    }
    assert (
        catalog["selic"] | {"rows": 2, "first_date": "2024-01-02", "last_date": "2024-01-03"}
        == catalog["selic"]
    )
    assert catalog["anbima_ima_b"]["rows"] is None

    latest = json.loads((tmp_path / "latest.json").read_text())
    assert latest["manifest"] == "snapshots/r1/manifest.json"
    assert json.loads((tmp_path / latest["manifest"]).read_text()) == manifest


def test_old_snapshots_are_pruned(con, tmp_path, registry):
    for run_id in ("r1", "r2", "r3"):
        _publish(con, tmp_path, registry, run_id=run_id, keep=2)

    index = json.loads((tmp_path / "index.json").read_text())
    assert {s["run_id"] for s in index["snapshots"]} == {"r2", "r3"}
    assert not list((tmp_path / "snapshots/r1").rglob("*.parquet"))
    assert (tmp_path / "snapshots/r3/manifest.json").exists()


def test_nothing_publishable_leaves_the_previous_snapshot_latest(con, tmp_path, registry):
    _publish(con, tmp_path, registry, run_id="r1")
    con.execute("DROP TABLE mart_macro_dashboard; DROP TABLE mart_di_curve_slope")

    with pytest.raises(PublishError):
        _publish(con, tmp_path, registry, run_id="r2")

    assert json.loads((tmp_path / "latest.json").read_text())["run_id"] == "r1"
    assert not (tmp_path / "snapshots/r2").exists()


def test_target_is_a_folder_or_a_bucket_on_the_lakehouse_store(tmp_path, monkeypatch):
    from ofl.publish import S3Target, default_target

    for name in (
        "OFL_PUBLISH_DIR",
        "OFL_PUBLISH_BUCKET",
        "OFL_PUBLISH_ENDPOINT",
        "OFL_PUBLISH_ACCESS_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(PublishError, match="OFL_PUBLISH_BUCKET"):
        default_target()

    # Only the bucket named: same store and credentials as the lakehouse.
    monkeypatch.setenv("OFL_PUBLISH_BUCKET", "ofl-public")
    target = default_target()
    assert isinstance(target, S3Target)
    assert target.bucket == "ofl-public"
    assert target.s3.meta.endpoint_url == "http://localhost:9000"

    monkeypatch.setenv("OFL_PUBLISH_ENDPOINT", "https://elsewhere.example")
    assert default_target().s3.meta.endpoint_url == "https://elsewhere.example"

    monkeypatch.setenv("OFL_PUBLISH_DIR", str(tmp_path))
    assert isinstance(default_target(), LocalTarget)
