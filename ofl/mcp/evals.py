"""Gold-query evals for the agent surface, scored against a frozen corpus.

Two properties make this a gate rather than a demo:

* It runs against the VERSIONED fixture, not against whatever the pipeline produced
  today, so a red score means the tools changed rather than the world.
* ``ofl evals`` exits non-zero below a committed threshold. "Prints a scoreboard" is an
  effect on stdout; a suite with zero failures also "includes the failures".

Cases live in ``tests/evals/*.yaml`` and are authored BEFORE the behaviour, which is the
only ordering in which a case can be a specification instead of a description.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ofl.mcp.tools import TOOLS, Refusal, ToolContext
from ofl.platform.logging import get_logger

log = get_logger(__name__)

REPO = Path(__file__).resolve().parent.parent.parent
EVALS_DIR = REPO / "tests" / "evals"
THRESHOLD_FILE = REPO / "evals" / "threshold.json"


@dataclass
class CaseResult:
    id: str
    passed: bool
    detail: str = ""


def context_from_corpus(corpus: Path | str, *, with_percentile: bool = True) -> ToolContext:
    """Build the agent's view from a frozen table corpus — the ONE way it is built.

    The CLI and the test suite call this same function. Two builders meant two corpora,
    and a gold case that passed against one and failed against the other was measuring
    the fixture rather than the tools.

    The percentile mart is materialised from the published recipe instead of being
    shipped as a second file, so the eval run exercises that SQL too and the corpus
    cannot drift away from the mart derived from it.
    """
    import duckdb
    import polars as pl

    from ofl.registry import load_registry

    con = duckdb.connect()
    tables: list[str] = []
    for path in sorted(list(Path(corpus).glob("*.csv")) + list(Path(corpus).glob("*.parquet"))):
        df = pl.read_parquet(path) if path.suffix == ".parquet" else pl.read_csv(path, try_parse_dates=True)
        con.register(path.stem, df.to_arrow())
        tables.append(path.stem)

    registry = load_registry()
    active = registry.active()
    series = [
        {
            "series_id": s.key, "name": s.name, "domain": s.domain, "unit": s.unit,
            "basis": s.basis, "scale": s.scale, "day_count": s.day_count,
            "horizon": s.horizon, "frequency": s.frequency, "provider": s.handler,
        }
        for s in active
    ]

    if with_percentile and "fact_observation" in tables:
        recipe = REPO / "ofl" / "release" / "recipes" / "percentile_asof.sql"
        con.register(
            "dim_series",
            pl.DataFrame(
                {"series_id": [s.key for s in active], "frequency": [s.frequency for s in active]}
            ).to_arrow(),
        )
        # `.arrow()` returns a streaming reader; registering one back on the connection
        # that feeds it is self-referential and hangs. Materialise first.
        computed = con.execute(recipe.read_text(encoding="utf-8")).pl()
        con.register("mart_series_percentile", computed.to_arrow())
        tables.append("mart_series_percentile")

    return ToolContext.from_catalog(con, series, tables)


def load_cases(directory: Path | None = None) -> list[dict[str, Any]]:
    d = directory or EVALS_DIR
    cases: list[dict[str, Any]] = []
    for path in sorted(d.glob("*.yaml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or []
        for case in payload:
            case.setdefault("_file", path.name)
            cases.append(case)
    return cases


def run_case(ctx: ToolContext, case: dict[str, Any]) -> CaseResult:
    tool = TOOLS.get(case["tool"])
    if tool is None:
        return CaseResult(case["id"], False, f"unknown tool {case['tool']!r}")

    args = dict(case.get("args") or {})
    positional = args.pop("_positional", None)
    try:
        out = tool(ctx, *( [positional] if positional is not None else [] ), **args)
    except Refusal as refusal:
        want = case.get("expect_refusal")
        if want is None:
            return CaseResult(case["id"], False, f"unexpected refusal: {refusal.code}")
        if refusal.code != want:
            return CaseResult(case["id"], False, f"refused {refusal.code!r}, wanted {want!r}")
        return CaseResult(case["id"], True, f"refused as specified ({want})")

    if case.get("expect_refusal"):
        return CaseResult(case["id"], False, f"expected refusal {case['expect_refusal']!r}, got an answer")

    for key, want in (case.get("expect") or {}).items():
        got = out.get(key)
        if isinstance(want, str) and want.startswith("~"):
            if want[1:].lower() not in str(got).lower():
                return CaseResult(case["id"], False, f"{key}: {got!r} does not contain {want[1:]!r}")
        elif got != want:
            return CaseResult(case["id"], False, f"{key}: got {got!r}, wanted {want!r}")
    return CaseResult(case["id"], True)


def threshold() -> float:
    if THRESHOLD_FILE.is_file():
        return float(json.loads(THRESHOLD_FILE.read_text(encoding="utf-8"))["min_pass_rate"])
    return 1.0


def run_evals(ctx: ToolContext, cases: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    cases = cases if cases is not None else load_cases()
    results = [run_case(ctx, c) for c in cases]
    passed = sum(1 for r in results if r.passed)
    rate = passed / len(results) if results else 0.0
    return {
        "total": len(results),
        "passed": passed,
        "failed": len(results) - passed,
        "pass_rate": rate,
        "threshold": threshold(),
        "ok": bool(results) and rate >= threshold(),
        # Failures are enumerated, not summarised. A scoreboard that omits them is a
        # scoreboard nobody can act on.
        "failures": [{"id": r.id, "detail": r.detail} for r in results if not r.passed],
    }
