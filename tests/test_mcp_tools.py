"""The agent surface: what it answers, and — more importantly — what it refuses.

The refusals are the tested behaviour. An agent that answers everything is an agent that
answers wrongly about the things it should not have touched, and the two failure modes
this suite pins are the ones that actually bite a macro text-to-SQL agent: serving a
source whose licence forbids it, and handing back a number without the unit that makes it
mean anything.
"""

import sys
from datetime import date

import duckdb
import polars as pl
import pytest

from ofl.mcp.tools import (
    Refusal,
    ToolContext,
    describe_series,
    explain_refusal,
    get_curve,
    get_observations,
    get_percentile,
    list_series,
    run_sql,
)

SERIES = [
    {"series_id": "selic", "name": "SELIC over", "domain": "rates", "unit": "percent",
     "basis": "per_day", "scale": 1, "day_count": "252", "horizon": "spot",
     "frequency": "daily", "provider": "bacen_sgs"},
    {"series_id": "divida_pib", "name": "Gross debt", "domain": "fiscal", "unit": "percent",
     "basis": "pct_of_gdp", "scale": 1, "day_count": "n/a", "horizon": "spot",
     "frequency": "monthly", "provider": "bacen_sgs"},
    {"series_id": "credito_total", "name": "Credit", "domain": "credit", "unit": "brl",
     "basis": "level", "scale": 1_000_000, "day_count": "n/a", "horizon": "spot",
     "frequency": "monthly", "provider": "bacen_sgs"},
    {"series_id": "focus_selic_fim_ano", "name": "Focus Selic", "domain": "rates",
     "unit": "percent", "basis": "per_year", "scale": 1, "day_count": "252",
     "horizon": "calendar_year_end", "frequency": "weekly", "provider": "bacen_focus"},
    # Restricted by verdict — present in the catalogue, not servable.
    {"series_id": "anbima_ima_b", "name": "IMA-B", "domain": "market", "unit": "index",
     "basis": "level", "scale": 1, "day_count": "n/a", "horizon": "spot",
     "frequency": "daily", "provider": "anbima"},
]


@pytest.fixture
def ctx():
    con = duckdb.connect()
    obs = pl.DataFrame(
        {
            "series_id": ["selic"] * 3 + ["divida_pib"],
            "date": [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 7, 1)],
            "value": [0.0512, 0.0512, 0.0513, 76.4],
        }
    )
    pct = pl.DataFrame(
        {
            "series_id": ["selic", "divida_pib"],
            "date": [date(2026, 8, 5), date(2026, 7, 1)],
            "window_label": ["10y", "10y"],
            "window_start": [date(2016, 8, 5), date(2016, 7, 1)],
            "value": [0.0513, 76.4],
            "pct_rank": [0.87, 0.44],
            "n_obs": [2400, 20],
            "n_expected": [2520, 120],
            "percentile_allowed": [True, False],
            "method": ["mid_rank", "mid_rank"],
        }
    )
    tre = pl.DataFrame(
        {
            "instrument_id": ["a", "b"],
            "bond": ["Tesouro IPCA+", "Tesouro IPCA+ com Juros Semestrais"],
            "maturity": [date(2035, 5, 15), date(2035, 5, 15)],
            "date": [date(2026, 8, 5)] * 2,
            "buy_rate": [7.10, 7.04],
            "sell_rate": [7.12, 7.06],
        }
    )
    con.register("fact_observation", obs.to_arrow())
    con.register("mart_series_percentile", pct.to_arrow())
    con.register("fact_tesouro_direto", tre.to_arrow())
    return ToolContext.from_catalog(
        con, SERIES, ["fact_observation", "mart_series_percentile", "fact_tesouro_direto"]
    )


def test_the_tools_import_without_the_protocol_sdk():
    """The split that makes every test below possible offline."""
    sys.modules.setdefault("mcp", None)
    import importlib

    mod = importlib.import_module("ofl.mcp.tools")
    assert not any(m.startswith("mcp.") for m in dir(mod))


def test_restricted_series_is_listed_but_not_served(ctx):
    """Listed, because a catalogue that hides what it cannot give is less trustworthy
    than one that says so."""
    out = list_series(ctx)
    assert "anbima_ima_b" not in {s["series_id"] for s in out["series"]}
    assert "anbima_ima_b" in {s["series_id"] for s in out["withheld"]}

    with pytest.raises(Refusal) as exc:
        describe_series(ctx, "anbima_ima_b")
    assert exc.value.code == "not_redistributable"
    assert "anbima" in str(exc.value)


def test_a_refusal_carries_the_written_verdict(ctx):
    out = explain_refusal(ctx, "anbima_ima_b")
    assert out["servable"] is False
    assert out["rights_holder"] == "anbima"
    assert out["license_id"] == "proprietary-anbima"
    assert out["verdict_date"]
    assert "sandbox" in out["reason"].lower() or "denied" in out["reason"].lower()


def test_an_unknown_series_is_refused_by_name(ctx):
    with pytest.raises(Refusal) as exc:
        describe_series(ctx, "nao_existe")
    assert exc.value.code == "unknown_series"


def test_describe_carries_the_whole_unit_tuple(ctx):
    """The unit confusion this exists to prevent: `selic` and `divida_pib` are both
    `percent`, and are not the same quantity."""
    a = describe_series(ctx, "selic")
    b = describe_series(ctx, "divida_pib")
    assert a["unit"] == b["unit"] == "percent"
    assert a["basis"] != b["basis"]
    assert "DAILY" in a["reading"]
    assert "ratio to GDP" in b["reading"]


def test_a_scaled_series_says_so_in_words(ctx):
    note = describe_series(ctx, "credito_total")["reading"]
    assert "1,000,000" in note


def test_an_expectation_says_it_is_one(ctx):
    assert "EXPECTATION" in describe_series(ctx, "focus_selic_fim_ano")["reading"]


def test_observations_carry_their_unit(ctx):
    """A caller that skipped `describe_series` is exactly the caller who will mix
    grandezas, so the payload does not depend on it having been called."""
    out = get_observations(ctx, "selic", limit=2)
    assert out["unit"] == "percent" and out["basis"] == "per_day"
    assert out["scale"] == 1
    assert len(out["observations"]) == 2
    assert out["truncated"] is True


def test_percentile_is_refused_when_the_window_cannot_support_one(ctx):
    ok = get_percentile(ctx, "selic")
    assert ok["pct_rank"] == 0.87 and ok["method"] == "mid_rank"

    with pytest.raises(Refusal) as exc:
        get_percentile(ctx, "divida_pib")
    assert exc.value.code == "insufficient_history"
    assert "20 of ~120" in str(exc.value)


def test_curve_is_keyed_by_instrument_not_by_bucket(ctx):
    """Two IPCA+ bonds at one maturity: a bond-type bucket would merge them."""
    out = get_curve(ctx)
    assert len({p["instrument_id"] for p in out["points"]}) == 2
    assert len({p["maturity"] for p in out["points"]}) == 1


@pytest.mark.parametrize(
    ("sql", "code"),
    [
        ("DROP TABLE fact_observation", "not_a_select"),
        ("SELECT 1; SELECT 2", "multi_statement"),
        ("SELECT * FROM fact_observation; DROP TABLE x", "multi_statement"),
        ("INSTALL httpfs", "not_a_select"),
        ("SELECT 1", "no_allowlisted_table"),
        ("SELECT * FROM read_csv('/etc/passwd')", "no_allowlisted_table"),
    ],
)
def test_the_escape_hatch_refuses_everything_it_should(ctx, sql, code):
    with pytest.raises(Refusal) as exc:
        run_sql(ctx, sql)
    assert exc.value.code == code


def test_the_escape_hatch_works_and_is_row_capped(ctx):
    out = run_sql(ctx, "SELECT series_id, value FROM fact_observation ORDER BY date", limit=2)
    assert out["columns"] == ["series_id", "value"]
    assert len(out["rows"]) == 2
    assert out["truncated"] is True
    assert out["tables_read"] == ["fact_observation"]


def test_the_allowlist_is_derived_from_the_catalogue_not_written_by_hand(ctx):
    """A hand-maintained allowlist drifts away from the verdicts it is meant to enforce."""
    narrowed = ToolContext.from_catalog(ctx.con, SERIES, ["fact_observation"])
    with pytest.raises(Refusal) as exc:
        get_curve(narrowed)
    assert exc.value.code == "table_unavailable"
