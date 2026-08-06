"""The gates a release must clear — and the one that must not block it.

Every gate here returns a named result. A negative acceptance test asserts the NAME, not
merely a non-zero exit: "the command failed" is satisfied by a missing directory just as
well as by a gate that bit, and a test that cannot tell those apart is not testing the
gate.

Freshness is deliberately absent. A source outage that blocks publication pins every
consumer to a stale release with NO SIGNAL, which is strictly worse than publishing the
old observation date and letting the consumer see it.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ofl.providers import DATA_CLASSES, is_redistributable, load_providers
from ofl.release.contracts import TableContract, compare

if TYPE_CHECKING:
    import polars as pl

RELEASE_ID = re.compile(r"^\d{4}-\d{2}-\d{2}\.\d+$")

#: Which `data_class` values a release of each class may contain.
CLASS_POLICY: dict[str, set[str]] = {
    "production": {"live"},
    "fixture": {"synthetic"},
}


@dataclass(frozen=True)
class GateResult:
    name: str
    status: str  # pass | fail
    table: str | None = None
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "pass"


def _ok(name: str, table: str | None = None) -> GateResult:
    return GateResult(name=name, status="pass", table=table)


def gate_release_id_format(release_id: str) -> GateResult:
    """``YYYY-MM-DD.N`` with a real date.

    A sentinel like ``0000-00-00.1`` is not a date — ``date.fromisoformat`` rejects it —
    so a release built with one would carry an identity nothing can order or parse.
    """
    import datetime as dt

    if not RELEASE_ID.match(release_id):
        return GateResult("release_id_format", "fail", detail=f"{release_id!r} is not YYYY-MM-DD.N")
    day, _, seq = release_id.rpartition(".")
    try:
        dt.date.fromisoformat(day)
    except ValueError:
        return GateResult("release_id_format", "fail", detail=f"{day!r} is not a calendar date")
    if int(seq) < 1:
        return GateResult("release_id_format", "fail", detail="the sequence starts at 1")
    return _ok("release_id_format")


def gate_contracts(table: str, df: "pl.DataFrame", contract: TableContract | None) -> GateResult:
    if contract is None:
        return GateResult("contracts", "fail", table, "no contract declared for this table")
    problems = compare(contract, df)
    if problems:
        return GateResult("contracts", "fail", table, "; ".join(problems))
    return _ok("contracts", table)


def gate_grain(table: str, df: "pl.DataFrame", primary_key: Sequence[str]) -> GateResult:
    """The declared key identifies a row, and no key component is null.

    This runs on the frame that will be PUBLISHED. Its twin —
    ``keys.assert_grain_is_not_coarser`` — runs upstream, before deduplication, and that
    is the half that can catch a key which is too coarse: after a dedup,
    ``count == count distinct`` holds by construction, precisely because the dedup
    removed the rows that would have shown the collision.
    """
    import polars as pl

    if not primary_key:
        return GateResult("grain", "fail", table, "no primary key declared")
    missing = [k for k in primary_key if k not in df.columns]
    if missing:
        return GateResult("grain", "fail", table, f"key column(s) absent: {missing}")

    nulls = {k: df[k].null_count() for k in primary_key}
    if any(nulls.values()):
        bad = {k: v for k, v in nulls.items() if v}
        return GateResult("grain", "fail", table, f"null key component(s): {bad}")

    dupes = df.group_by(list(primary_key)).len().filter(pl.col("len") > 1)
    if dupes.height:
        return GateResult(
            "grain",
            "fail",
            table,
            f"{dupes.height} duplicate key(s); sample {dupes.head(2).to_dicts()}",
        )
    return _ok("grain", table)


def gate_required(table: str, df: "pl.DataFrame", contract: TableContract | None) -> GateResult:
    """Columns the contract marks ``required`` carry no nulls.

    This is where "provider is mandatory" is actually enforced — the column is nullable
    in Delta because it has to be, so the obligation has to live somewhere that can check
    values rather than storage flags.
    """
    if contract is None:
        return GateResult("required", "fail", table, "no contract declared")
    offenders = {
        name: df[name].null_count()
        for name in contract.required_columns()
        if name in df.columns and df[name].null_count()
    }
    absent = [name for name in contract.required_columns() if name not in df.columns]
    if absent:
        return GateResult("required", "fail", table, f"required column(s) absent: {absent}")
    if offenders:
        return GateResult("required", "fail", table, f"nulls in required column(s): {offenders}")
    return _ok("required", table)


def gate_class(table: str, df: "pl.DataFrame", release_class: str) -> GateResult:
    """``data_class`` compatible with the release class.

    The structural half comes first: a table with no ``data_class`` column fails here
    rather than passing for want of anything to test. A predicate over a column that may
    not exist is satisfied by its absence, which is how a derived table with no
    provenance sails through every check that was meant to stop it.
    """
    allowed = CLASS_POLICY.get(release_class)
    if allowed is None:
        return GateResult("class", "fail", table, f"unknown release_class {release_class!r}")
    if "data_class" not in df.columns:
        return GateResult("class", "fail", table, "no `data_class` column")
    present = set(df["data_class"].unique().to_list())
    unknown = present - set(DATA_CLASSES)
    if unknown:
        return GateResult("class", "fail", table, f"data_class outside the domain: {sorted(unknown)}")
    bad = present - allowed
    if bad:
        return GateResult(
            "class", "fail", table, f"{release_class} release carries data_class {sorted(bad)}"
        )
    return _ok("class", table)


def _providers_in(df: "pl.DataFrame") -> set[str] | None:
    """Provenance values, whether the column is scalar or an aggregated array."""
    import polars as pl

    if "provider" in df.columns:
        col = df["provider"]
        if col.dtype == pl.List:
            return set(col.explode().drop_nulls().unique().to_list())
        return set(col.drop_nulls().unique().to_list())
    if "providers" in df.columns:
        return set(df["providers"].explode().drop_nulls().unique().to_list())
    return None


def gate_license(table: str, df: "pl.DataFrame") -> GateResult:
    """No non-redistributable provenance — and the domain is checked first.

    Without the domain check, "no restricted provider found" is satisfied by a misspelled
    provider exactly as well as by a clean release: the gate would pass for the wrong
    reason. A table with no provenance column at all fails structurally, for the same
    reason as the class gate.
    """
    present = _providers_in(df)
    if present is None:
        return GateResult("license", "fail", table, "no `provider`/`providers` column")
    if "provider" in df.columns and df["provider"].null_count():
        return GateResult("license", "fail", table, "null provenance values")

    known = set(load_providers().handlers)
    unknown = sorted(present - known)
    if unknown:
        return GateResult("license", "fail", table, f"provider(s) outside the registry: {unknown}")

    blocked = sorted(p for p in present if not is_redistributable(p))
    if blocked:
        return GateResult("license", "fail", table, f"non-redistributable provider(s): {blocked}")
    return _ok("license", table)


def gate_key_drift(
    table: str, keys_removed: int, rows_previous: int, *, threshold: float = 0.01
) -> GateResult:
    """A mass re-keying is an event, not a quiet diff.

    The pipeline is full-refresh, so a change in a source's label spelling re-keys the
    whole history at once. ``revised_rows_vs_previous`` cannot see it — that field matches
    rows BY key, and when every key changes, nothing matches and it reports zero revisions
    in the exact release where 100% of the identities moved.
    """
    if rows_previous <= 0:
        return _ok("key_drift", table)
    ratio = keys_removed / rows_previous
    if ratio > threshold:
        return GateResult(
            "key_drift",
            "fail",
            table,
            f"{keys_removed}/{rows_previous} keys disappeared ({ratio:.1%} > {threshold:.0%})",
        )
    return _ok("key_drift", table)
