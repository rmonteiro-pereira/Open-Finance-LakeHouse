"""Invariants of the reader surface, asserted where the CI actually runs.

The adversarial review's verdict on the original acceptance was blunt and correct: all
four conditions described properties of a Next.js app, and the declared CI is a single
pytest job with no Node. A criterion nothing executes is a criterion that cannot fail.

So the invariants live here, over the SOURCE and the data contract, and the Node build
gets its own job. Each assertion below is written to be falsifiable: the route check is
set EQUALITY, not containment (containment would pass with the old routes still present),
and the host scan asserts non-vacuity before it asserts absence.
"""

import json
import re
from pathlib import Path

import pytest

DASH = Path(__file__).resolve().parent.parent / "dashboard"
APP = DASH / "src" / "app"

#: The six routes, named after questions somebody asks out loud rather than after marts.
EXPECTED_ROUTES = {"/", "/juro-real", "/inflacao", "/curva-do-tesouro", "/serie/[series_id]", "/confianca"}

#: Gone by product decision (they answered no question a reader asks) AND, for the first
#: two, by licence: B3 barred redistribution of derived values absent written authorisation.
WITHDRAWN_ROUTES = {"/derivatives", "/equities", "/fx", "/yield-curve", "/catalog", "/real-interest", "/inflation"}


def _code(path: Path) -> str:
    """Source with comments stripped.

    Written after two of these assertions failed on their own explanatory comments: a
    naive substring check cannot tell "this file uses `sla_state`" from "this file
    explains why it does not". Grepping prose is not testing behaviour.
    """
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return "\n".join(re.sub(r"//.*$", "", line) for line in text.splitlines())


def _routes() -> set[str]:
    out = set()
    for page in APP.rglob("page.tsx"):
        rel = page.parent.relative_to(APP).as_posix()
        out.add("/" if rel == "." else f"/{rel}")
    return out


def test_the_route_set_is_exactly_the_six():
    """Equality, not containment. Containment passes with every old route still shipping,
    which is the failure mode of "we added the new pages"."""
    assert _routes() == EXPECTED_ROUTES


def test_the_withdrawn_routes_are_gone():
    assert _routes() & WITHDRAWN_ROUTES == set()


def test_the_build_is_a_static_export():
    config = (DASH / "next.config.ts").read_text(encoding="utf-8")
    assert 'output: "export"' in config
    assert '"standalone"' not in config


def test_no_source_file_reaches_the_cluster():
    """Non-vacuity first: assert there is something to scan, then that the host is absent.

    Scanning an empty set for a forbidden string passes trivially, and the previous form
    of this check would have passed on the day the sources were deleted.
    """
    sources = [p for p in (DASH / "src").rglob("*.ts*") if p.is_file()]
    assert len(sources) > 10, "nothing was scanned — the check would pass vacuously"
    offenders = [p.name for p in sources if "svc.cluster.local" in p.read_text(encoding="utf-8")]
    assert offenders == []


def test_the_synthetic_producer_is_gone_and_the_release_producer_is_not():
    """F0 removed the synthetic SNAPSHOT from git; this removes the GENERATOR. Leaving it
    keeps one command away a site full of invented numbers that look exactly like real
    ones."""
    assert not (DASH / "snapshot" / "gen_synthetic.py").exists()
    # The MinIO reader goes too: a surface that needs the cluster dies with the cluster.
    assert not (DASH / "snapshot" / "export.py").exists()
    assert (DASH / "snapshot" / "from_release.py").is_file()


def test_the_data_layer_has_no_silent_fallback():
    """`readJsonOr(name, fallback)` renders an empty chart for missing data, and an empty
    chart is indistinguishable from a quiet market."""
    lib = _code(DASH / "src" / "lib" / "release.ts")
    assert "readJsonOr" not in lib, "the forgiving loader is back"
    assert "No fallback exists by design" in lib, "the thrown error must say so"
    assert not (DASH / "src" / "lib" / "data.ts").exists()


def test_every_rendered_number_goes_through_the_slot_component():
    """The freshness chip is part of `Stat`, so "no number without an as-of date" is a
    property of the component rather than a habit of whoever wrote the page."""
    slot = (DASH / "src" / "components" / "slot.tsx").read_text(encoding="utf-8")
    assert 'data-slot="value"' in slot
    assert "FreshnessChip" in slot
    # Every page that shows a figure imports it.
    for route in ("juro-real", "inflacao"):
        page = (APP / route / "page.tsx").read_text(encoding="utf-8")
        assert "@/components/slot" in page


def test_pages_use_marked_slots_not_free_prose():
    """A marked slot is assertable; a generated sentence is a fluent way to be wrong."""
    for route in ("juro-real", "curva-do-tesouro", "confianca"):
        page = (APP / route / "page.tsx").read_text(encoding="utf-8")
        assert re.search(r'data-slot="[a-z-]+"', page), route


def test_the_unit_guard_compares_the_whole_tuple():
    """Keyed on `unit` alone the guard passes for the SELIC daily rate against monthly
    IPCA — both are `percent` — which is the confusion it exists to end."""
    slot = (DASH / "src" / "components" / "slot.tsx").read_text(encoding="utf-8")
    assert "assertComparable" in slot
    assert "${s!.unit}|${s!.basis}|${s!.scale}" in slot


def test_the_trust_page_is_generated_from_the_manifest():
    page = (APP / "confianca" / "page.tsx").read_text(encoding="utf-8")
    assert "getMeta" in page
    # No hand-written figure: the page cannot be made to look healthier by editing it.
    assert not re.search(r">\s*\d+\s*(gates?|portões)\s*<", page)


def test_the_freshness_verdict_is_computed_not_read():
    """A manifest that declares its own `sla_state: "ok"` is correct until it stops being
    republished — precisely when the reader needs it to say otherwise."""
    lib = _code(DASH / "src" / "lib" / "release.ts")
    assert "export function freshness(" in lib
    # The verdict must be COMPUTED here, never read from a field the producer wrote.
    assert "sla_state" not in lib


# ------------------------------------------------------------------ the data producer


def test_from_release_refuses_a_release_with_failing_gates(tmp_path):
    import sys

    sys.path.insert(0, str(DASH / "snapshot"))
    from from_release import ReleaseUnusable, build  # noqa: PLC0415

    rel = tmp_path / "rel"
    rel.mkdir()
    (rel / "manifest.json").write_text(
        json.dumps(
            {
                "release_id": "1970-01-01.1",
                "release_class": "fixture",
                "generated_at": "1970-01-01T00:00:00Z",
                "gates": [{"name": "license", "status": "fail", "table": "x", "detail": "blocked"}],
                "data_assets": [],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ReleaseUnusable, match="failing gates"):
        build(rel, out_dir=tmp_path / "out")


def test_from_release_writes_generated_from_release(tmp_path):
    """The gate the site build reads. `generated_from` says "release" AND which one, so a
    stale page can be traced to the artefact that produced it."""
    import sys

    import polars as pl

    sys.path.insert(0, str(DASH / "snapshot"))
    from from_release import build  # noqa: PLC0415

    from ofl.release.build import build_release, read_source
    from ofl.release.contracts import load_contracts

    fixtures = Path(__file__).parent / "fixtures" / "release"
    rel = tmp_path / "rel"
    build_release(
        read_source(fixtures, ["fact_observation", "fact_tesouro_direto"]),
        load_contracts(fixtures / "contracts"),
        release_id="1970-01-01.1",
        release_class="fixture",
        out_dir=rel,
    )
    # The percentile mart is not in this corpus; the producer must say so rather than
    # emitting a page with a silently missing panel.
    with pytest.raises(Exception, match="does not declare table"):
        build(rel, out_dir=tmp_path / "out")

    pl.DataFrame({"series_id": ["ipca"], "date": ["2026-01-01"], "window_label": ["10y"],
                  "pct_rank": [0.5], "n_obs": [30], "percentile_allowed": [True]}).write_parquet(
        rel / "parquet" / "mart_series_percentile.parquet"
    )
    manifest = json.loads((rel / "manifest.json").read_text(encoding="utf-8"))
    manifest["data_assets"].append(
        {"name": "mart_series_percentile", "status": "published", "rows": 1,
         "primary_key": ["series_id", "date", "window_label"], "sha256": "x",
         "path": "parquet/mart_series_percentile.parquet"}
    )
    (rel / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    out = tmp_path / "out"
    build(rel, out_dir=out)
    meta = json.loads((out / "_meta.json").read_text(encoding="utf-8"))
    assert meta["generated_from"] == "release"
    assert meta["release_id"] == "1970-01-01.1"
    assert meta["series"], "the site needs the series block to render a freshness chip"
