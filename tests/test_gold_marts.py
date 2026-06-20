"""Deterministic gold-mart checks: known inputs -> hand-calculated outputs."""

from datetime import date

import duckdb
import polars as pl
import pytest

from ofl.transform.gold.runner import _model_sql, execute_models

MONTHS = [(2024, m) for m in range(1, 13)] + [(2025, 1)]  # 13 monthly points
IPCA_12M = (1.01**12 - 1) * 100  # 12 months at 1% MoM


@pytest.fixture
def con():
    rows = []
    for sid, v in {"ipca": 1.0, "ipca_15": 0.9, "inpc": 0.8, "igp_m": 1.2, "igp_di": 1.1, "igp_10": 1.3}.items():
        rows += [(sid, date(y, m, 1), v) for y, m in MONTHS]
    for y, m in MONTHS:
        rows.append(("selic_meta", date(y, m, 1), 13.0))
        rows.append(("divida_pib", date(y, m, 1), 75.0))
    for d, v in [(2, 5.00), (3, 5.05), (4, 4.95), (5, 5.00)]:
        rows.append(("usd_brl", date(2024, 1, d), v))
    fact = pl.DataFrame(rows, schema=["series_id", "date", "value"], orient="row")

    treasury = pl.DataFrame(
        {
            "bond": ["Tesouro IPCA+ 2029", "Tesouro Prefixado 2027"],
            "maturity": [date(2029, 8, 15), date(2027, 1, 1)],
            "date": [date(2024, 1, 2), date(2024, 1, 2)],
            "buy_rate": [5.5, 10.5],
            "sell_rate": [5.6, 10.6],
            "buy_price": [1234.56, 900.0],
            "sell_price": [1233.0, 899.0],
        }
    )

    c = duckdb.connect()
    c.register("fact_observation", fact.to_arrow())
    c.register("fact_treasury", treasury.to_arrow())
    return c


def test_all_marts_execute(con):
    counts = execute_models(con, write=False)
    assert set(counts) == {
        "mart_real_interest",
        "mart_inflation_panel",
        "mart_fx",
        "mart_macro_dashboard",
        "mart_yield_curve",
    }
    assert all(v > 0 for v in counts.values())


def test_yield_curve_tenor(con):
    yc = con.execute(_model_sql("mart_yield_curve")).pl()
    ipca = yc.filter(pl.col("bond_type") == "ipca_plus").row(0, named=True)
    assert ipca["yield"] == 5.6
    assert ipca["years_to_maturity"] == pytest.approx(5.62, abs=0.05)


def test_real_interest_matches_hand_calc(con):
    row = con.execute(_model_sql("mart_real_interest")).pl().tail(1).row(0, named=True)
    expected = ((1 + 13.0 / 100) / (1 + IPCA_12M / 100) - 1) * 100
    assert row["ipca_accum_12m"] == pytest.approx(IPCA_12M, abs=1e-6)
    assert row["real_interest_rate"] == pytest.approx(expected, abs=1e-6)


def test_inflation_panel_accumulation(con):
    row = con.execute(_model_sql("mart_inflation_panel")).pl().tail(1).row(0, named=True)
    assert row["ipca_12m"] == pytest.approx(IPCA_12M, abs=1e-6)
    assert row["igpm_12m"] == pytest.approx((1.012**12 - 1) * 100, abs=1e-6)


def test_fx_daily_return(con):
    fx = con.execute(_model_sql("mart_fx")).pl().filter(pl.col("series_id") == "usd_brl").sort("date")
    assert fx.row(1, named=True)["daily_return_pct"] == pytest.approx(1.0, abs=1e-9)
