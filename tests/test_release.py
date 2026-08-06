"""The release artefact — built, then attacked.

Every negative assertion here names the gate it expects. "The command failed" is
satisfied by a missing directory exactly as well as by a gate that bit, and a test that
cannot tell those apart does not test the gate. Each canary mutates ONE thing away from
a corpus that is known to pass, so a failure has one candidate cause.

The corpus itself is checked for being adversarial (``test_the_corpus_is_adversarial``):
a fixture without the discriminating pair would let every other assertion pass while
proving nothing.
"""

import json
import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from ofl.release.build import ReleaseError, build_release, read_source
from ofl.release.contracts import load_contracts
from ofl.release.verify import EXIT_GATE, EXIT_OK, UsageError, verify_release, write_report

FIXTURES = Path(__file__).parent / "fixtures" / "release"
ALLOWLIST = ["fact_tesouro_direto", "fact_observation"]
FIXTURE_ID = "1970-01-01.1"  # a valid date, obviously not production


def _load():
    return read_source(FIXTURES, ALLOWLIST), load_contracts(FIXTURES / "contracts")


def _build(tmp_path, tables=None, contracts=None, **kw):
    t, c = _load()
    return build_release(
        tables if tables is not None else t,
        contracts if contracts is not None else c,
        release_id=kw.pop("release_id", FIXTURE_ID),
        release_class=kw.pop("release_class", "fixture"),
        out_dir=tmp_path / "out",
        **kw,
    )


# ------------------------------------------------------------------ the corpus itself


def test_the_corpus_is_adversarial():
    tables, _ = _load()
    tre = tables["fact_tesouro_direto"]
    obs = tables["fact_observation"]

    # The pair the shipped key collapsed: same maturity, labels differing only by coupon.
    same_maturity = tre.filter(pl.col("maturity") == date(2035, 5, 15))
    assert same_maturity["instrument_id"].n_unique() == 2
    assert same_maturity["bond"].n_unique() == 2

    # Two frequencies, so a single-cadence assumption cannot hide in the corpus.
    assert {"ipca", "selic"} <= set(obs["series_id"])
    # A short series, so the percentile coverage floor has something to refuse.
    assert obs.filter(pl.col("series_id") == "ibc_br").height < 12
    # Honest authenticity: the corpus says what it is.
    assert set(obs["data_class"]) == {"synthetic"}


# ------------------------------------------------------------------------ happy path


def test_build_produces_a_complete_release(tmp_path):
    manifest = _build(tmp_path)
    out = tmp_path / "out"

    assert manifest["release_class"] == "fixture"
    assert manifest["publishable"] is False
    assert (out / "manifest.json").is_file()
    assert (out / "checksums.sha256").is_file()
    for name in ALLOWLIST:
        assert (out / "parquet" / f"{name}.parquet").is_file()
        assert (out / "contracts" / f"{name}.contract.json").is_file()

    assert all(g["status"] == "pass" for g in manifest["gates"])
    published = {a["name"]: a for a in manifest["data_assets"]}
    assert published["fact_tesouro_direto"]["primary_key"] == ["instrument_id", "date"]
    assert published["fact_tesouro_direto"]["status"] == "published"
    assert published["fact_tesouro_direto"]["sha256"]


def test_verify_accepts_the_fixture_release_when_asked_about_fixtures(tmp_path):
    _build(tmp_path)
    report = verify_release(tmp_path / "out", expect_class="fixture")
    assert report.ok and report.exit_code == EXIT_OK
    assert report.failed_gate is None
    assert report.checked_files, "a verify that checked nothing must not report success"


def test_a_bare_verify_asks_about_production_and_refuses_the_fixture(tmp_path):
    """Both halves matter. Without the default being `production`, a fixture would pass a
    bare verify just by declaring itself a fixture — the gate would only confirm the
    label it was handed."""
    _build(tmp_path)
    report = verify_release(tmp_path / "out")
    assert not report.ok
    assert report.failed_gate == "class"
    assert report.exit_code == EXIT_GATE


def test_freshness_inputs_are_published_and_the_verdict_is_not(tmp_path):
    manifest = _build(tmp_path)
    series = {s["series_id"]: s for s in manifest["series"]}
    sample = series["selic"]
    assert sample["freshness_budget_hours"] > 0
    assert "last_observation_date" in sample
    # No verdict anywhere: a manifest that declares itself "ok" lies on precisely the day
    # the producer dies, while one carrying only inputs keeps answering correctly.
    assert "sla_state" not in sample
    assert "sla_state" not in json.dumps(manifest)


def test_series_freshness_is_at_the_series_grain_not_the_table(tmp_path):
    manifest = _build(tmp_path)
    assert len(manifest["series"]) > 10
    for asset in manifest["data_assets"]:
        assert "last_observation_date" not in asset or asset.get("path") is None


# ---------------------------------------------------------------------- the canaries


def test_release_id_must_be_a_real_date(tmp_path):
    with pytest.raises(ReleaseError) as exc:
        _build(tmp_path, release_id="0000-00-00.1")
    assert exc.value.gate == "release_id_format"


def test_duplicate_primary_key_fails_the_grain_gate(tmp_path):
    tables, contracts = _load()
    tre = tables["fact_tesouro_direto"]
    tables["fact_tesouro_direto"] = pl.concat([tre, tre.head(1)])
    with pytest.raises(ReleaseError) as exc:
        _build(tmp_path, tables=tables, contracts=contracts)
    assert exc.value.gate == "grain"
    assert exc.value.table == "fact_tesouro_direto"


def test_null_key_component_fails_the_grain_gate(tmp_path):
    tables, contracts = _load()
    tables["fact_tesouro_direto"] = tables["fact_tesouro_direto"].with_columns(
        pl.when(pl.col("bond") == "Tesouro Prefixado")
        .then(None)
        .otherwise(pl.col("instrument_id"))
        .alias("instrument_id")
    )
    with pytest.raises(ReleaseError) as exc:
        _build(tmp_path, tables=tables, contracts=contracts)
    assert exc.value.gate == "grain"


def test_an_extra_column_fails_the_contracts_gate(tmp_path):
    """Set equality, not containment: this is what obliges a MINOR bump instead of
    letting a new column appear in a release unannounced."""
    tables, contracts = _load()
    tables["fact_observation"] = tables["fact_observation"].with_columns(
        pl.lit("surprise").alias("undeclared")
    )
    with pytest.raises(ReleaseError) as exc:
        _build(tmp_path, tables=tables, contracts=contracts)
    assert exc.value.gate == "contracts"


def test_a_restricted_provider_fails_the_licence_gate(tmp_path):
    tables, contracts = _load()
    tables["fact_observation"] = tables["fact_observation"].with_columns(
        pl.when(pl.col("series_id") == "selic")
        .then(pl.lit("anbima"))
        .otherwise(pl.col("provider"))
        .alias("provider")
    )
    with pytest.raises(ReleaseError) as exc:
        _build(tmp_path, tables=tables, contracts=contracts)
    assert exc.value.gate == "license"


def test_a_misspelled_provider_fails_before_the_licence_question(tmp_path):
    """Otherwise "no restricted provider present" is satisfied by a typo just as well as
    by a clean release, and the gate passes for the wrong reason."""
    tables, contracts = _load()
    tables["fact_observation"] = tables["fact_observation"].with_columns(
        pl.lit("bacen_sqs").alias("provider")
    )
    with pytest.raises(ReleaseError) as exc:
        _build(tmp_path, tables=tables, contracts=contracts)
    assert exc.value.gate == "license"
    assert "outside the registry" in str(exc.value)


def test_live_rows_fail_a_fixture_release_and_synthetic_fails_a_production_one(tmp_path):
    tables, contracts = _load()
    tables["fact_observation"] = tables["fact_observation"].with_columns(
        pl.lit("live").alias("data_class")
    )
    with pytest.raises(ReleaseError) as exc:
        _build(tmp_path, tables=tables, contracts=contracts)
    assert exc.value.gate == "class"

    tables2, contracts2 = _load()
    with pytest.raises(ReleaseError) as exc2:
        _build(tmp_path, tables=tables2, contracts=contracts2, release_class="production", supersedes=None)
    # The production path refuses before the class gate: identity comes first.
    assert exc2.value.gate in ("supersedes", "class")


def test_a_production_release_must_state_what_it_supersedes(tmp_path):
    with pytest.raises(ReleaseError) as exc:
        _build(tmp_path, release_class="production")
    assert exc.value.gate == "supersedes"


def test_a_table_outside_the_allowlist_aborts_the_read(tmp_path):
    """Not skipped — aborted. The only `.duckdb` producer in this repo copies every mart
    out of MinIO with no filter, so "ignore what I do not recognise" is the shape a leak
    takes."""
    src = tmp_path / "src"
    shutil.copytree(FIXTURES, src)
    (src / "mart_di_curve_points.csv").write_text("a\n1\n", encoding="utf-8")
    with pytest.raises(ReleaseError) as exc:
        read_source(src, ALLOWLIST)
    assert exc.value.gate == "reader_allowlist"
    assert exc.value.table == "mart_di_curve_points"


def test_nothing_is_written_when_a_gate_bites(tmp_path):
    """A half-written directory is indistinguishable from a good release to anything that
    only checks for files."""
    tables, contracts = _load()
    tables["fact_observation"] = tables["fact_observation"].with_columns(
        pl.lit("anbima").alias("provider")
    )
    with pytest.raises(ReleaseError):
        _build(tmp_path, tables=tables, contracts=contracts)
    assert not (tmp_path / "out" / "manifest.json").exists()


# ------------------------------------------------------- the verifier, independently


def test_verify_rejects_a_table_with_no_provenance_column(tmp_path):
    """The structural half. Both licence mechanisms are predicates over a column that may
    not exist, and a predicate over an absent column is satisfied by its absence."""
    _build(tmp_path)
    out = tmp_path / "out"
    stripped = pl.read_parquet(out / "parquet" / "fact_observation.parquet").drop("provider")
    stripped.write_parquet(out / "parquet" / "fact_observation.parquet")

    report = verify_release(out, expect_class="fixture")
    assert not report.ok
    assert report.failed_gate == "license"
    assert "no provenance column" in report.detail


def test_verify_rejects_a_table_with_no_data_class_column(tmp_path):
    _build(tmp_path)
    out = tmp_path / "out"
    stripped = pl.read_parquet(out / "parquet" / "fact_observation.parquet").drop("data_class")
    stripped.write_parquet(out / "parquet" / "fact_observation.parquet")

    report = verify_release(out, expect_class="fixture")
    assert not report.ok
    assert report.failed_gate == "class"


def test_verify_reads_every_data_file_not_only_parquet(tmp_path):
    """A CSV smuggled into the release directory is checked like anything else — an
    earlier scope of "each parquet" left goldens and the DuckDB mirror unexamined."""
    _build(tmp_path)
    out = tmp_path / "out"
    pl.DataFrame({"a": [1]}).write_csv(out / "smuggled.csv")

    report = verify_release(out, expect_class="fixture")
    assert not report.ok
    assert report.failed_gate == "license"
    assert "smuggled.csv" in report.detail


def test_verify_rejects_an_internally_inconsistent_manifest(tmp_path):
    """A fixture that claims to be publishable is broken regardless of what was asked."""
    _build(tmp_path)
    path = tmp_path / "out" / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["publishable"] = True
    path.write_text(json.dumps(manifest), encoding="utf-8")

    report = verify_release(tmp_path / "out", expect_class="fixture")
    assert not report.ok
    assert report.failed_gate == "class"


def test_a_missing_directory_is_a_usage_error_not_a_gate_failure(tmp_path):
    """The distinction the exit codes exist for: exit 2 means the question was malformed,
    exit 3 means the answer was no."""
    with pytest.raises(UsageError):
        verify_release(tmp_path / "nope")


def test_verify_report_names_the_gate_on_disk(tmp_path):
    _build(tmp_path)
    report = verify_release(tmp_path / "out")  # production expectation -> fails
    path = write_report(report, tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["failed_gate"] == "class"
    assert payload["ok"] is False
