"""The unit tuple — defect E, and the reason a presence test would not have closed it.

``unit`` alone carried ``percent`` for the SELIC daily rate (~0.05), monthly IPCA
variation (~0.4), the unemployment level (~7) and debt/GDP (~75). Four quantities, one
label. Requiring a ``basis`` field to *exist* would let all four keep the same wrong
value and still pass, so the assertions below are about the values.

The registry is read RAW here, with ``yaml.safe_load``, not through ``load_registry``.
Every field on the ``Series`` model has a permissive default, so "this series has no
scale" is not representable in the parsed object — a test over the model cannot fail on
an omission it silently fills in.
"""

import csv
from pathlib import Path

import pytest
import yaml

from ofl.registry import load_registry

REPO = Path(__file__).resolve().parent.parent
REGISTRY = REPO / "sources" / "registry.yml"
GOLDEN = REPO / "tests" / "fixtures" / "registry_units.expected.csv"

UNITS = {"percent", "brl", "usd", "index", "contracts", "none"}
BASES = {"per_day", "per_month", "per_year", "level", "pct_of_gdp", "mom", "yoy"}
SCALES = {1, 1000, 1000000, 1000000000}
DAY_COUNTS = {"252", "360", "365", "calendar", "n/a"}
HORIZONS = {"spot", "rolling_12m", "calendar_year_end", "n/a"}


def _raw() -> dict:
    return yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))["series"]


def _golden() -> dict[str, dict[str, str]]:
    lines = [ln for ln in GOLDEN.read_text(encoding="utf-8").splitlines() if not ln.startswith("#")]
    return {r["series_id"]: r for r in csv.DictReader(lines)}


def _observation(raw: dict) -> dict:
    return {k: v for k, v in raw.items() if v.get("fact", "observation") == "observation"}


def test_registry_matches_the_golden_tuple():
    """Drift check. A basis changed in the registry has to be argued in a review."""
    raw, golden = _raw(), _golden()
    assert set(raw) == set(golden), "series added or removed without updating the golden"

    diffs = []
    for key, body in sorted(raw.items()):
        g = golden[key]
        actual = (
            str(body.get("unit", "")),
            str(body.get("basis") or ""),
            str(body.get("scale") or ""),
            str(body.get("day_count") or ""),
            str(body.get("horizon") or ""),
        )
        expected = (g["unit"], g["basis"], g["scale"], g["day_count"], g["horizon"])
        if actual != expected:
            diffs.append((key, expected, actual))
    assert not diffs, f"registry drifted from the golden: {diffs}"


def test_every_observation_series_declares_the_full_tuple():
    """Read raw: the model's defaults would answer for an omitted key."""
    missing = {
        key: [f for f in ("basis", "scale", "day_count", "horizon") if f not in body]
        for key, body in _observation(_raw()).items()
        if any(f not in body for f in ("basis", "scale", "day_count", "horizon"))
    }
    assert not missing, f"observation series missing unit fields: {missing}"


def test_group_series_declare_per_column_instead_of_a_fake_tuple():
    """A registry row that describes a GROUP of symbols does not describe a quantity.
    Giving it one tuple would be a lie with a schema behind it."""
    raw = _raw()
    others = {k: v for k, v in raw.items() if v.get("fact", "observation") != "observation"}
    assert len(others) == 11
    for key, body in others.items():
        assert body.get("unit_scope") == "per_column", key
        assert "basis" not in body, key


def test_domains_are_closed():
    for key, body in _observation(_raw()).items():
        assert body["unit"] in UNITS, (key, body["unit"])
        assert body["basis"] in BASES, (key, body["basis"])
        assert body["scale"] in SCALES, (key, body["scale"])
        assert str(body["day_count"]) in DAY_COUNTS, (key, body["day_count"])
        assert body["horizon"] in HORIZONS, (key, body["horizon"])


def test_the_two_selics_are_not_the_same_quantity():
    """The named trap. SGS 11 is the effective DAILY rate; SGS 1178 is its annualised
    twin. Both were `unit: percent`, and an earlier draft of the RFC labelled the daily
    one `per_year` in its own worked example."""
    raw = _raw()
    assert raw["selic"]["sgs_id"] == 11
    assert raw["over"]["sgs_id"] == 1178
    assert raw["selic"]["basis"] == "per_day"
    assert raw["over"]["basis"] == "per_year"
    assert raw["selic"]["unit"] == raw["over"]["unit"] == "percent"


def test_millions_carry_a_scale_instead_of_claiming_reais():
    """`brl_million -> brl` without a scale makes the contract assert a unit the value
    does not have, and the consumer multiplies by 1e6 in the wrong direction."""
    raw = _raw()
    for key in ("resultado_primario", "credito_total", "ipea_pib", "ipea_nfsp_primario"):
        assert raw[key]["unit"] == "brl" and raw[key]["scale"] == 1_000_000, key
    assert raw["reservas_internacionais"]["unit"] == "usd"
    assert raw["reservas_internacionais"]["scale"] == 1_000_000


def test_an_expectation_is_distinguishable_from_a_spot_rate():
    """Same unit, same basis — only `horizon` separates the Focus median for year-end
    from the policy rate in force today."""
    raw = _raw()
    focus, spot = raw["focus_selic_fim_ano"], raw["selic_meta"]
    assert (focus["unit"], focus["basis"]) == (spot["unit"], spot["basis"])
    assert focus["horizon"] == "calendar_year_end"
    assert spot["horizon"] == "spot"


def test_a_percent_label_no_longer_implies_a_comparable_quantity():
    """The defect, stated as a property: `percent` spans several bases, and that is now
    visible in the data instead of being a trap for whoever plots two of them."""
    bases = {b["basis"] for b in _observation(_raw()).values() if b["unit"] == "percent"}
    assert len(bases) > 1
    assert {"per_day", "mom", "level", "pct_of_gdp"} <= bases


def test_the_loader_exposes_the_tuple():
    series = load_registry().series
    assert series["selic"].basis == "per_day"
    assert series["reservas_internacionais"].scale == 1_000_000
    assert series["tesouro_direto"].unit_scope == "per_column"


@pytest.mark.parametrize("field", ["basis", "scale", "day_count", "horizon"])
def test_the_golden_would_notice_a_wrong_value_not_just_a_missing_one(field):
    """Proves the drift check bites: mutate one field and the comparison must fail."""
    raw, golden = _raw(), _golden()
    key = "ipca"
    mutated = dict(raw[key])
    mutated[field] = "WRONG" if field != "scale" else 999
    actual = tuple(str(mutated.get(f) or "") for f in ("unit", "basis", "scale", "day_count", "horizon"))
    g = golden[key]
    expected = (g["unit"], g["basis"], g["scale"], g["day_count"], g["horizon"])
    assert actual != expected
