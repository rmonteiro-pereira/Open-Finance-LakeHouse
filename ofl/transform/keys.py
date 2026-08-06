"""Natural keys for the conformed facts — pure Python/Polars, importable without Spark.

Why this module exists
----------------------
``conform_treasury`` merged on ``t.bond = s.bond AND t.date = s.date`` and deduplicated
with ``partitionBy("bond","date")``. ``maturity`` sat in the row and *outside the key*, so
two rows that differ only by maturity collided and one was dropped — no error, no log, no
metric. The key asserted a grain the table did not have.

Two properties make the fix real rather than nominal:

* **The key is data, not a string inside a PySpark call.** The CI installs the core extras
  only, so anything expressed inside a Spark session cannot be tested. ``TREASURY_KEY`` is
  a module-level tuple consumed by both the Spark merge and the pure functions here, so a
  test exercises the same tuple production uses.
* **The identifier has one normative pre-image, evaluated once.** ``instrument_id`` is
  computed in the bronze landing, in Polars, and carried downstream unchanged. Spark never
  recomputes it. A second implementation would diverge on NULL alone: ``a || NULL``
  propagates NULL in Spark and DuckDB, while an f-string yields ``sha1("...|None")`` — a
  perfectly non-null hash for a row with no maturity.

``instrument_id_expr`` publishes the same pre-image as a SQL expression so the agreement
between languages is a test rather than a claim.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

#: Grain of ``silver.fact_treasury``.
#:
#: The date column keeps the name ``date``: renaming it to ``ref_date`` would be a MAJOR
#: contract break across four marts and three tests, bought for nothing — the defect was
#: the *missing* key component, not the name of the one that was there.
TREASURY_KEY: tuple[str, ...] = ("instrument_id", "date")

#: Separator for the identifier pre-image. Chosen because it cannot occur in a Tesouro or
#: ANBIMA bond label; :func:`instrument_id` rejects any component containing it rather
#: than escaping, so the pre-image stays trivially reversible by a consumer.
_SEP = "|"

_WHITESPACE = re.compile(r"\s+")


class GrainError(ValueError):
    """The declared key does not identify a row in the source.

    Raised *before* deduplication. Once rows have been deduplicated, a key that is too
    coarse satisfies ``COUNT(*) = COUNT(DISTINCT key)`` **by construction** — precisely
    because the dedup deleted the rows that would have shown the collision. A grain check
    that runs after the dedup cannot fail on the defect it exists to catch.
    """


def normalize_label(label: str) -> str:
    """Collapse whitespace runs and strip — the only normalization applied to a label.

    Deliberately conservative. It repairs the trailing spaces and double spaces a
    semicolon-separated CSV produces, and it does **not** try to repair spelling: two
    genuinely different labels must keep two different identifiers, because a label change
    at the source is an event the release has to surface (``keys_added`` / ``keys_removed``),
    not one to paper over.
    """
    return _WHITESPACE.sub(" ", label).strip()


def instrument_id(provider: str, bond_label: str, maturity: dt.date | None) -> str:
    """``sha1(provider|bond_label|maturity)`` — the one normative pre-image.

    ``bond_label`` is the **raw** label published by the source (``Tipo Titulo``), never a
    derived bucket. The only bucket that exists in this repo is the three-way
    ``CASE ... ILIKE`` in ``mart_yield_curve.sql``, and it collapses "Tesouro IPCA+ 2035"
    (NTN-B Principal, zero-coupon) with "Tesouro IPCA+ com Juros Semestrais 2035" — same
    maturity, same bucket, one row survives. Keying on the bucket would reintroduce the
    exact defect this function exists to remove.

    A missing maturity raises. It is reachable — the Tesouro extractor parses the column
    with ``strict=False`` and only drops null *dates* — and every silent alternative is
    worse: a null identifier withholds the whole release, and coalescing one collapses
    every malformed row into a single fictitious instrument.
    """
    if maturity is None:
        raise ValueError(
            f"maturity is required to identify an instrument (provider={provider!r}, "
            f"bond_label={bond_label!r}); a row without it cannot be keyed"
        )
    parts = [provider.strip(), normalize_label(bond_label), maturity.isoformat()]
    for part in parts[:2]:
        if not part:
            raise ValueError(f"empty key component in {parts!r}")
        if _SEP in part:
            raise ValueError(f"key component {part!r} contains the reserved separator {_SEP!r}")
    return hashlib.sha1(_SEP.join(parts).encode("utf-8")).hexdigest()  # noqa: S324


def instrument_id_expr(
    provider: str = "provider", bond_label: str = "bond", maturity: str = "maturity"
) -> str:
    """The same pre-image as a SQL expression (DuckDB / Spark dialect-compatible).

    Exists so that "Python and SQL agree" is a test — ``tests/test_keys.py`` evaluates this
    against the frozen vectors in DuckDB — rather than an assertion in a document.
    """
    return (
        f"sha1(concat_ws('{_SEP}', trim({provider}), "
        f"regexp_replace(trim({bond_label}), '\\s+', ' ', 'g'), "
        f"strftime({maturity}, '%Y-%m-%d')))"
    )


def with_instrument_id(
    df: "pl.DataFrame",
    *,
    provider: str,
    bond_col: str = "bond",
    maturity_col: str = "maturity",
    out: str = "instrument_id",
) -> "pl.DataFrame":
    """Attach ``instrument_id`` at the bronze landing — the only place it is computed.

    Downstream lanes carry the column; Spark never recomputes the hash. One identifier
    with two implementations is how the NULL semantics diverge between languages, and the
    Python side is the forgiving one (``f"...|None"`` hashes fine), so it is the side that
    would ship the defect.

    Row-wise on purpose: :func:`instrument_id` raises on a missing maturity or a label
    containing the reserved separator, and a landing that fails loudly on a malformed row
    is the point.
    """
    import polars as pl

    return df.with_columns(
        pl.struct(bond_col, maturity_col)
        .map_elements(
            lambda s: instrument_id(provider, s[bond_col], s[maturity_col]),
            return_dtype=pl.String,
        )
        .alias(out)
    )


def merge_condition(keys: Sequence[str] = TREASURY_KEY, target: str = "t", source: str = "s") -> str:
    """Build the Delta ``MERGE`` predicate from the key tuple.

    The predicate used to be a literal string inside the PySpark call, which put the
    project's most consequential grain decision somewhere no test could reach. Built from
    the tuple, it is exercised by the same assertions as everything else here.
    """
    if not keys:
        raise ValueError("a merge condition needs at least one key column")
    return " AND ".join(f"{target}.{k} = {source}.{k}" for k in keys)


def assert_grain_is_not_coarser(df: "pl.DataFrame", keys: Sequence[str] = TREASURY_KEY) -> None:
    """Gate ``grain``, part (i): the key identifies a row **in the source**.

    Run this on the union of bronze, before any deduplication. If two source rows share a
    key, that is a declared-grain violation and the run must fail — deduplicating them is
    how the original defect deleted data without saying so.
    """
    import polars as pl

    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise GrainError(f"key column(s) absent from the frame: {missing}")

    duplicated = df.group_by(list(keys)).len().filter(pl.col("len") > 1).sort("len", descending=True)
    if duplicated.height:
        sample = duplicated.head(3).to_dicts()
        raise GrainError(
            f"key {tuple(keys)} does not identify a row: {duplicated.height} colliding "
            f"key(s) in {df.height} rows. Sample: {sample}"
        )


def dedup_latest(
    df: "pl.DataFrame",
    keys: Sequence[str] = TREASURY_KEY,
    *,
    order_by: str = "ingested_at",
    tiebreak: str = "load_id",
) -> "pl.DataFrame":
    """Keep the most recently ingested row per key, with the tie-break declared.

    "Latest" needs a column to be latest *by*, and a tie in that column needs a second
    criterion — otherwise the surviving row depends on frame order, which is
    non-determinism published as data. If a tie survives both ``order_by`` and
    ``tiebreak``, this raises instead of picking one.
    """
    import polars as pl

    sort_cols = [c for c in (order_by, tiebreak) if c in df.columns]
    if not sort_cols:
        raise ValueError(f"neither {order_by!r} nor {tiebreak!r} is present; 'latest' is undefined")

    ordered = df.sort(sort_cols, descending=True, maintain_order=True)
    winners = ordered.unique(subset=list(keys), keep="first", maintain_order=True)

    contenders = ordered.join(winners.select([*keys, *sort_cols]), on=[*keys, *sort_cols], how="inner")
    ties = contenders.group_by(list(keys)).len().filter(pl.col("len") > 1)
    if ties.height:
        raise ValueError(
            f"{ties.height} key(s) tie on {sort_cols} and cannot be ordered deterministically. "
            f"Sample: {ties.head(3).to_dicts()}"
        )
    return winners
