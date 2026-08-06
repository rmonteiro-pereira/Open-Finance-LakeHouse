"""The six agent tools — pure functions over a DuckDB connection, no protocol SDK.

The split matters more than the tools do. Anything that imports the MCP SDK can only be
exercised by speaking the protocol; these take a connection and return a dict, so the
evals run in the ordinary test suite with nothing installed. ``server.py`` is the shell.

**The failure this exists to prevent.** A text-to-SQL agent over Brazilian macro does not
usually fail for want of data — it fails for want of SEMANTICS. It reads ``unit: percent``
on `selic` (the effective DAILY rate, ~0.05) and on `divida_pib` (a debt/GDP ratio, ~75)
and treats them as one quantity. So ``describe_series`` returns the whole unit tuple, and
every observation payload carries it: publishing the data without the semantics only
trades "nobody can ask" for "anyone can receive a wrong number that looks right", which is
worse.

**Refusal is a behaviour, not an accident.** The allowlist is DERIVED from the catalogue
at construction time, and a refusal carries the licence verdict that caused it. A hand-
written allowlist drifts from the verdicts it is supposed to enforce; a refusal with no
reason is indistinguishable from a bug.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ofl.providers import load_providers

if TYPE_CHECKING:
    import duckdb

MAX_ROWS = 5_000
DEFAULT_ROWS = 500

#: Statements the escape hatch may run. Everything else is refused before parsing.
_SQL_ALLOWED_PREFIX = "select"
_SQL_FORBIDDEN = (
    "attach", "copy", "create", "delete", "drop", "export", "insert", "install",
    "load", "pragma", "update", "install", "set ",
)


class Refusal(Exception):
    """A refusal with a stated reason. Never a bare failure."""

    def __init__(self, reason: str, *, code: str, detail: str = ""):
        super().__init__(reason)
        self.code = code
        self.detail = detail

    def as_dict(self) -> dict[str, Any]:
        return {"refused": True, "code": self.code, "reason": str(self), "detail": self.detail}


@dataclass(frozen=True)
class ToolContext:
    """What the tools may see. Built once, at boot, from the release."""

    con: "duckdb.DuckDBPyConnection"
    series: dict[str, dict[str, Any]]
    tables: frozenset[str]

    @classmethod
    def from_catalog(
        cls, con: "duckdb.DuckDBPyConnection", series: list[dict[str, Any]], tables: list[str]
    ) -> "ToolContext":
        return cls(con=con, series={s["series_id"]: s for s in series}, tables=frozenset(tables))


def _require_series(ctx: ToolContext, series_id: str) -> dict[str, Any]:
    meta = ctx.series.get(series_id)
    if meta is None:
        raise Refusal(
            f"unknown series {series_id!r}",
            code="unknown_series",
            detail="use list_series to see what is available",
        )
    provider = meta.get("provider", "")
    entry = load_providers().get(provider)
    if entry is None or not entry.redistribute:
        holder = entry.rights_holder if entry else "unknown"
        licence = entry.license_id if entry else "unverified"
        raise Refusal(
            f"series {series_id!r} may not be served: {holder} ({licence}) does not permit "
            "redistribution",
            code="not_redistributable",
            detail=(entry.notes.strip() if entry else "no written verdict for this handler"),
        )
    return meta


def list_series(ctx: ToolContext, *, domain: str | None = None) -> dict[str, Any]:
    """What exists here — and, deliberately, what exists but cannot be served."""
    providers = load_providers()
    items, withheld = [], []
    for sid, meta in sorted(ctx.series.items()):
        if domain and meta.get("domain") != domain:
            continue
        entry = providers.get(meta.get("provider", ""))
        row = {
            "series_id": sid,
            "name": meta.get("name", ""),
            "domain": meta.get("domain"),
            "unit": meta.get("unit"),
            "basis": meta.get("basis"),
            "frequency": meta.get("frequency"),
        }
        if entry and entry.redistribute:
            items.append(row)
        else:
            # Listed, not hidden. A catalogue that declares what it cannot give is more
            # trustworthy than one that pretends the series does not exist.
            withheld.append({**row, "state": entry.state if entry else "unverified"})
    return {"series": items, "withheld": withheld, "count": len(items)}


def describe_series(ctx: ToolContext, series_id: str) -> dict[str, Any]:
    """The semantic dictionary for one series — the antidote to unit confusion."""
    meta = _require_series(ctx, series_id)
    return {
        "series_id": series_id,
        "name": meta.get("name", ""),
        "domain": meta.get("domain"),
        "unit": meta.get("unit"),
        "basis": meta.get("basis"),
        "scale": meta.get("scale", 1),
        "day_count": meta.get("day_count"),
        "horizon": meta.get("horizon"),
        "frequency": meta.get("frequency"),
        "provider": meta.get("provider"),
        "reading": _reading_note(meta),
    }


def _reading_note(meta: dict[str, Any]) -> str:
    """One sentence an answer can quote, so the caveat travels with the number."""
    unit, basis = meta.get("unit"), meta.get("basis")
    scale = meta.get("scale", 1) or 1
    parts = []
    if basis == "per_day":
        parts.append("a DAILY rate — annualise before comparing with a per-year series")
    elif basis == "mom":
        parts.append("month-over-month variation, not a level")
    elif basis == "pct_of_gdp":
        parts.append("a ratio to GDP, not a rate")
    elif basis == "level":
        parts.append("a level, not a variation")
    if scale != 1:
        parts.append(f"values are in units of {scale:,} {unit}")
    if meta.get("horizon") not in (None, "spot"):
        parts.append(f"an EXPECTATION ({meta['horizon']}), not a realised value")
    return "; ".join(parts) or "a spot value"


def get_observations(
    ctx: ToolContext,
    series_id: str,
    *,
    start: str | None = None,
    end: str | None = None,
    limit: int = DEFAULT_ROWS,
) -> dict[str, Any]:
    meta = _require_series(ctx, series_id)
    limit = max(1, min(int(limit), MAX_ROWS))
    sql = "SELECT date, value FROM fact_observation WHERE series_id = ?"
    params: list[Any] = [series_id]
    if start:
        sql += " AND date >= ?"
        params.append(start)
    if end:
        sql += " AND date <= ?"
        params.append(end)
    sql += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    rows = ctx.con.execute(sql, params).fetchall()
    return {
        "series_id": series_id,
        # The unit travels with every payload, not only with `describe_series`. A caller
        # that skipped the description is exactly the caller that will mix grandezas.
        "unit": meta.get("unit"),
        "basis": meta.get("basis"),
        "scale": meta.get("scale", 1),
        "observations": [{"date": str(d), "value": v} for d, v in rows],
        "truncated": len(rows) == limit,
    }


def get_percentile(
    ctx: ToolContext, series_id: str, *, window_label: str = "10y", as_of: str | None = None
) -> dict[str, Any]:
    """The as-of percentile, with the refusal when the window does not support one."""
    _require_series(ctx, series_id)
    if window_label not in ("5y", "10y", "full"):
        raise Refusal(f"unknown window {window_label!r}", code="unknown_window")
    if "mart_series_percentile" not in ctx.tables:
        # A missing table is a refusal with a name, not a stack trace. An agent can relay
        # "this release does not carry percentiles"; it cannot relay a CatalogException.
        raise Refusal(
            "this release does not carry the percentile mart", code="table_unavailable"
        )
    sql = (
        "SELECT date, value, pct_rank, n_obs, n_expected, percentile_allowed, method, window_start "
        "FROM mart_series_percentile WHERE series_id = ? AND window_label = ?"
    )
    params: list[Any] = [series_id, window_label]
    if as_of:
        sql += " AND date <= ?"
        params.append(as_of)
    sql += " ORDER BY date DESC LIMIT 1"
    row = ctx.con.execute(sql, params).fetchone()
    if row is None:
        raise Refusal(
            f"no percentile available for {series_id!r} over {window_label}",
            code="no_percentile",
        )
    date, value, pct, n_obs, n_expected, allowed, method, window_start = row
    if not allowed:
        raise Refusal(
            f"{series_id!r} has insufficient history for a {window_label} percentile "
            f"({n_obs} of ~{n_expected} expected observations)",
            code="insufficient_history",
            detail="a percentile over too few points is a number, and publishing it is worse "
            "than publishing nothing",
        )
    return {
        "series_id": series_id,
        "date": str(date),
        "value": value,
        "pct_rank": pct,
        "window_label": window_label,
        "window_start": str(window_start),
        "n_obs": n_obs,
        "method": method,
    }


def get_curve(ctx: ToolContext, *, ref_date: str | None = None) -> dict[str, Any]:
    """The Tesouro curve for a trading day, keyed by instrument.

    Keyed by `instrument_id`, never by a bond-type bucket: the bucket merges the
    zero-coupon and the semiannual-coupon bond of one index at the same maturity.
    """
    if "fact_tesouro_direto" not in ctx.tables:
        raise Refusal("the treasury table is not in this release", code="table_unavailable")
    sql = "SELECT instrument_id, bond, maturity, date, buy_rate, sell_rate FROM fact_tesouro_direto"
    params: list[Any] = []
    if ref_date:
        sql += " WHERE date = ?"
        params.append(ref_date)
    else:
        sql += " WHERE date = (SELECT max(date) FROM fact_tesouro_direto)"
    sql += " ORDER BY maturity"
    rows = ctx.con.execute(sql, params).fetchall()
    return {
        "ref_date": str(rows[0][3]) if rows else None,
        "points": [
            {
                "instrument_id": r[0],
                "bond": r[1],
                "maturity": str(r[2]),
                "buy_rate_pct_pa": r[4],
                "sell_rate_pct_pa": r[5],
            }
            for r in rows
        ],
    }


def explain_refusal(ctx: ToolContext, series_id: str) -> dict[str, Any]:
    """Why a series is not served — the verdict, in the words that were written down."""
    meta = ctx.series.get(series_id)
    if meta is None:
        return {"series_id": series_id, "known": False, "reason": "not in this release's catalogue"}
    entry = load_providers().get(meta.get("provider", ""))
    if entry is None:
        return {
            "series_id": series_id,
            "known": True,
            "servable": False,
            "state": "unverified",
            "reason": "no written licence verdict for this handler; the default is deny",
        }
    return {
        "series_id": series_id,
        "known": True,
        "servable": entry.redistribute,
        "state": entry.state,
        "rights_holder": entry.rights_holder,
        "license_id": entry.license_id,
        "url": entry.url,
        "verdict_date": entry.verdict_date,
        "reason": entry.notes.strip() or ("open data" if entry.redistribute else "redistribution not permitted"),
    }


def run_sql(ctx: ToolContext, sql: str, *, limit: int = DEFAULT_ROWS) -> dict[str, Any]:
    """The escape hatch — last, not first, and narrow.

    Read-only, single statement, allowlisted tables, row-capped. It exists because a
    catalogue can never anticipate every question, not because SQL is the interface.
    """
    text = sql.strip().rstrip(";")
    lowered = text.lower()
    if ";" in text:
        raise Refusal("one statement per call", code="multi_statement")
    if not lowered.startswith(_SQL_ALLOWED_PREFIX) and not lowered.startswith("with"):
        raise Refusal("only SELECT is allowed", code="not_a_select")
    for word in _SQL_FORBIDDEN:
        if word in lowered:
            raise Refusal(f"{word.strip()!r} is not allowed", code="forbidden_keyword")
    referenced = {t for t in ctx.tables if t in lowered}
    if not referenced:
        raise Refusal(
            f"the query names no allowlisted table (allowed: {sorted(ctx.tables)})",
            code="no_allowlisted_table",
        )
    limit = max(1, min(int(limit), MAX_ROWS))
    rel = ctx.con.execute(f"SELECT * FROM ({text}) LIMIT {limit}")
    cols = [d[0] for d in rel.description]
    rows = rel.fetchall()
    return {
        "columns": cols,
        "rows": [dict(zip(cols, r, strict=True)) for r in rows],
        "truncated": len(rows) == limit,
        "tables_read": sorted(referenced),
    }


#: Typed so a dispatcher can call these without mypy losing the signature.
TOOLS: dict[str, Callable[..., dict[str, Any]]] = {
    "list_series": list_series,
    "describe_series": describe_series,
    "get_observations": get_observations,
    "get_percentile": get_percentile,
    "get_curve": get_curve,
    "explain_refusal": explain_refusal,
    "run_sql": run_sql,
}
