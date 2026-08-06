"""The grain of ``fact_treasury`` — the defect that motivated the product RFC.

The old key was ``(bond, date)``. ``maturity`` sat in the row and outside the key, so two
rows that differed only by maturity collided and one was dropped silently. These tests are
written so that they FAIL under the old key: a test that passes either way would have been
worth nothing, which is what the previous test suite demonstrated — both
``tests/test_tesouro.py`` and ``tests/test_gold_marts.py`` use ``bond = "Tesouro IPCA+
2029"``, with the year baked into the label, so the collision never appears.
"""

import csv
import datetime as dt
from pathlib import Path

import duckdb
import polars as pl
import pytest

from ofl.transform.keys import (
    TREASURY_KEY,
    GrainError,
    assert_grain_is_not_coarser,
    dedup_latest,
    instrument_id,
    instrument_id_expr,
    merge_condition,
    normalize_label,
)

VECTORS = Path(__file__).parent / "fixtures" / "golden" / "instrument_id_vectors.csv"

#: The key that shipped. Too coarse: it merges two maturities that share a label.
LEGACY_KEY = ("bond", "date")

#: The key the FIRST proposed fix would have used. Coarser still — `bond_type` is the
#: three-way `CASE ... ILIKE` from `mart_yield_curve.sql`, so it also merges the
#: zero-coupon and the semiannual-coupon bond of the same index.
BUCKET_KEY = ("bond_type", "date")


def _vectors() -> list[dict[str, str]]:
    with VECTORS.open(encoding="utf-8") as fh:
        rows = [ln for ln in fh if not ln.startswith("#")]
    return list(csv.DictReader(rows))


def _bond_type(label: str) -> str:
    """The bucket exactly as `mart_yield_curve.sql` defines it."""
    low = label.lower()
    if "ipca" in low:
        return "ipca_plus"
    if "prefixado" in low:
        return "prefixado"
    if "selic" in low:
        return "selic"
    return "other"


def _discriminating_frame() -> pl.DataFrame:
    """A frame that collides under BOTH wrong keys, in different places.

    Rows 0/1 share a label and differ by maturity — the collision the shipped key causes.
    Rows 0/1/2 share a bucket — the collision the first proposed fix would have caused.
    """
    rows = [
        # (bond, maturity, date, sell_rate)
        ("Tesouro IPCA+", dt.date(2035, 5, 15), dt.date(2026, 8, 4), 7.10),
        ("Tesouro IPCA+", dt.date(2029, 8, 15), dt.date(2026, 8, 4), 7.32),
        ("Tesouro IPCA+ com Juros Semestrais", dt.date(2035, 5, 15), dt.date(2026, 8, 4), 7.05),
        ("Tesouro Prefixado", dt.date(2027, 1, 1), dt.date(2026, 8, 4), 13.90),
    ]
    df = pl.DataFrame(rows, schema=["bond", "maturity", "date", "sell_rate"], orient="row")
    return df.with_columns(
        pl.struct("bond", "maturity")
        .map_elements(
            lambda s: instrument_id("tesouro", s["bond"], s["maturity"]), return_dtype=pl.String
        )
        .alias("instrument_id"),
        pl.col("bond").map_elements(_bond_type, return_dtype=pl.String).alias("bond_type"),
    )


# --------------------------------------------------------------------------- vectors


def test_frozen_vectors_match_python():
    for v in _vectors():
        got = instrument_id(v["provider"], v["bond_label"], dt.date.fromisoformat(v["maturity"]))
        assert got == v["instrument_id"], v["pre_image"]


def test_frozen_vectors_match_duckdb():
    """Python and SQL agree — as a test, not as a claim in a document.

    Two implementations of the same identifier is how the NULL semantics diverge:
    ``a || NULL`` propagates NULL in SQL, an f-string yields ``sha1("...|None")``.
    """
    con = duckdb.connect()
    expr = instrument_id_expr("provider", "bond", "maturity")
    for v in _vectors():
        got = con.execute(
            f"SELECT {expr} FROM (SELECT ? AS provider, ? AS bond, CAST(? AS DATE) AS maturity)",
            [v["provider"], v["bond_label"], v["maturity"]],
        ).fetchone()[0]
        assert got == v["instrument_id"], v["pre_image"]


def test_vectors_include_the_pairs_that_the_legacy_key_collapses():
    """Guards the fixture itself: a vector file without the discriminating pair would let
    every other assertion here pass while proving nothing."""
    by_maturity: dict[str, set[str]] = {}
    for v in _vectors():
        by_maturity.setdefault(v["maturity"], set()).add(v["instrument_id"])
    assert any(len(ids) > 1 for ids in by_maturity.values())


# ------------------------------------------------------------------- the key discriminates


def test_new_key_discriminates_and_the_legacy_key_does_not():
    """The double assertion. Either half alone is satisfiable by a fixture that proves
    nothing: "the new key has no collisions" is true of a one-row frame, and "the old key
    collides" is true of a frame the new key cannot separate either."""
    df = _discriminating_frame()

    assert df.select(TREASURY_KEY).n_unique() == df.height, "new key must identify every row"

    collisions = df.group_by(list(LEGACY_KEY)).len().filter(pl.col("len") > 1)
    assert collisions.height == 1, "the fixture must actually collide under the shipped key"
    assert collisions["len"][0] == 2


def test_the_first_proposed_fix_would_have_been_worse_than_the_defect():
    """Keying on `bond_type` — the correction this RFC originally proposed — merges three
    instruments where the shipped key merged two. Three independent reviewers found this
    before it was written; the test is here so a fourth does not have to."""
    df = _discriminating_frame()

    bucket_collisions = df.group_by(list(BUCKET_KEY)).len().filter(pl.col("len") > 1)
    legacy_collisions = df.group_by(list(LEGACY_KEY)).len().filter(pl.col("len") > 1)

    assert bucket_collisions["len"].max() == 3
    assert bucket_collisions["len"].max() > legacy_collisions["len"].max()


def test_grain_gate_fires_on_both_wrong_keys():
    df = _discriminating_frame()
    assert_grain_is_not_coarser(df, TREASURY_KEY)  # passes
    for wrong in (LEGACY_KEY, BUCKET_KEY):
        with pytest.raises(GrainError, match="does not identify a row"):
            assert_grain_is_not_coarser(df, wrong)


def test_grain_gate_runs_before_dedup_not_after():
    """The gate must see what the dedup would eat.

    Deduplicating first makes ``COUNT(*) = COUNT(DISTINCT key)`` true *by construction* —
    a grain gate placed after the dedup is satisfied precisely because the evidence was
    deleted. This test pins the ordering by showing the post-dedup frame passes the very
    check that the pre-dedup frame fails.
    """
    df = _discriminating_frame().with_columns(
        pl.lit(dt.datetime(2026, 8, 4, 3, 0)).alias("ingested_at"), pl.lit("L1").alias("load_id")
    )
    with pytest.raises(GrainError):
        assert_grain_is_not_coarser(df, LEGACY_KEY)

    deduped = dedup_latest(df, LEGACY_KEY, tiebreak="sell_rate")
    assert_grain_is_not_coarser(deduped, LEGACY_KEY)  # now vacuously true
    assert deduped.height == 3, "the dedup silently ate an instrument"
    assert df.height - deduped.height == 1


def test_missing_maturity_raises_instead_of_hashing_none():
    with pytest.raises(ValueError, match="maturity is required"):
        instrument_id("tesouro", "Tesouro IPCA+", None)


def test_reserved_separator_and_empty_components_are_rejected():
    with pytest.raises(ValueError, match="reserved separator"):
        instrument_id("tesouro", "Tesouro|IPCA+", dt.date(2035, 5, 15))
    with pytest.raises(ValueError, match="empty key component"):
        instrument_id("  ", "Tesouro IPCA+", dt.date(2035, 5, 15))


def test_whitespace_normalization_is_conservative():
    assert normalize_label("  Tesouro   IPCA+  ") == "Tesouro IPCA+"
    assert instrument_id("tesouro", "  Tesouro   IPCA+ ", dt.date(2035, 5, 15)) == instrument_id(
        "tesouro", "Tesouro IPCA+", dt.date(2035, 5, 15)
    )
    # It repairs spacing; it does NOT repair spelling — a real label change must produce a
    # new id, because that is an event the release has to surface.
    assert instrument_id("tesouro", "Tesouro IPCA +", dt.date(2035, 5, 15)) != instrument_id(
        "tesouro", "Tesouro IPCA+", dt.date(2035, 5, 15)
    )


# ------------------------------------------------------------------------------ dedup


def _two_ingests(second_ts: dt.datetime, second_load: str = "L2") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "instrument_id": ["a", "a"],
            "date": [dt.date(2026, 8, 4)] * 2,
            "sell_rate": [7.10, 7.20],
            "ingested_at": [dt.datetime(2026, 8, 4, 3, 0), second_ts],
            "load_id": ["L1", second_load],
        }
    )


def test_dedup_keeps_the_most_recent_ingest():
    out = dedup_latest(_two_ingests(dt.datetime(2026, 8, 4, 9, 0)))
    assert out.height == 1
    assert out["sell_rate"][0] == 7.20


def test_dedup_breaks_a_timestamp_tie_by_load_id():
    out = dedup_latest(_two_ingests(dt.datetime(2026, 8, 4, 3, 0), second_load="L2"))
    assert out.height == 1
    assert out["load_id"][0] == "L2"


def test_dedup_raises_when_the_tie_cannot_be_broken():
    """An unbroken tie makes the surviving row depend on frame order — non-determinism
    published as data."""
    df = _two_ingests(dt.datetime(2026, 8, 4, 3, 0), second_load="L1")
    with pytest.raises(ValueError, match="cannot be ordered deterministically"):
        dedup_latest(df)


def test_dedup_without_an_ordering_column_is_an_error():
    df = pl.DataFrame({"instrument_id": ["a"], "date": [dt.date(2026, 8, 4)]})
    with pytest.raises(ValueError, match="'latest' is undefined"):
        dedup_latest(df)


# ---------------------------------------------------------------------- merge condition


def test_merge_condition_is_built_from_the_key_tuple():
    assert merge_condition(TREASURY_KEY) == "t.instrument_id = s.instrument_id AND t.date = s.date"
    assert merge_condition(("a",), target="x", source="y") == "x.a = y.a"
    with pytest.raises(ValueError):
        merge_condition(())


def test_silver_consumes_the_same_tuple():
    """``conform_treasury`` must key on the tuple these tests exercise — otherwise the
    suite tests a copy of the decision instead of the decision."""
    from ofl.transform.spark import silver

    assert silver.TREASURY_KEY is TREASURY_KEY
