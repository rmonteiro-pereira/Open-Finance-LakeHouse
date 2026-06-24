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

    sec = pl.DataFrame(
        {
            "symbol": ["PETR4"] * 3 + ["VALE3"] * 3,
            "date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)] * 2,
            "open": [10.0, 10.5, 11.0, 50.0, 55.0, 54.0],
            "high": [10.6, 10.8, 11.2, 56.0, 56.0, 55.0],
            "low": [9.9, 10.4, 10.8, 49.0, 53.0, 53.0],
            "close": [10.0, 10.5, 10.0, 50.0, 55.0, 55.0],
            "volume": [1000.0, 1200.0, 900.0, 2000.0, 2100.0, 1800.0],
        }
    )

    # B3 derivatives: DI1 Jan/27 futures over two sessions (OI, quotes, instrument).
    oi = pl.DataFrame(
        {
            "symbol": ["DI1F27", "DI1F27", "DI1F28"],
            "date": [date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 19)],
            "asset": ["DI1", "DI1", "DI1"],
            "expiration_code": ["F27", "F27", "F28"],
            "segment": ["FINANCIAL"] * 3,
            "open_interest": [6769291.0, 6851297.0, 4043295.0],
            "open_interest_var": [12000.0, 82006.0, -39430.0],
        }
    )
    dq = pl.DataFrame(
        {
            "symbol": ["DI1F27", "DI1F27", "DI1F28"],
            "date": [date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 19)],
            "segment": ["FINANCIAL"] * 3,
            "last_price": [14.20, 14.255, 14.82],
            "settlement_price": [93100.0, 93109.37, 80928.81],
            "settlement_rate": [14.21, 14.256, 14.814],
            "ref_price": [None, None, None],
            "contracts": [200000.0, 286656.0, 344548.0],
            "notional": [1.8e10, 2.6691e10, 2.79e10],
        }
    )
    inst = pl.DataFrame(
        {
            "symbol": ["DI1F27", "DI1F28"],
            "asset": ["DI1", "DI1"],
            "expiration_date": [date(2027, 1, 4), date(2028, 1, 3)],
            "contract_multiplier": [1.0, 1.0],
            "trading_ccy": ["BRL", "BRL"],
        }
    )

    c = duckdb.connect()
    c.register("fact_observation", fact.to_arrow())
    c.register("fact_treasury", treasury.to_arrow())
    c.register("fact_security_price", sec.to_arrow())
    c.register("fact_open_interest", oi.to_arrow())
    c.register("fact_derivatives_quote", dq.to_arrow())
    c.register("dim_instrument", inst.to_arrow())
    return c


def test_all_marts_execute(con):
    counts = execute_models(con, write=False)
    assert set(counts) == {
        "mart_real_interest",
        "mart_inflation_panel",
        "mart_fx",
        "mart_macro_dashboard",
        "mart_yield_curve",
        "mart_equity_daily",
        "mart_futures_curve",
        "mart_open_interest",
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


def test_futures_curve_joins_quote_oi_and_maturity(con):
    fc = (
        con.execute(_model_sql("mart_futures_curve")).pl()
        .filter((pl.col("symbol") == "DI1F27") & (pl.col("date") == date(2026, 6, 19)))
        .row(0, named=True)
    )
    assert fc["asset"] == "DI1"
    assert fc["settlement_rate"] == pytest.approx(14.256, abs=1e-9)
    assert fc["open_interest"] == pytest.approx(6851297.0, abs=1e-9)
    assert fc["maturity"] == date(2027, 1, 4)
    # 2026-06-19 -> 2027-01-04 is 199 days out.
    assert fc["days_to_maturity"] == 199


def test_open_interest_aggregates_by_asset(con):
    oi = (
        con.execute(_model_sql("mart_open_interest")).pl()
        .filter((pl.col("asset") == "DI1") & (pl.col("date") == date(2026, 6, 19)))
        .row(0, named=True)
    )
    # Two DI1 maturities trade on 2026-06-19: 6851297 + 4043295.
    assert oi["total_open_interest"] == pytest.approx(6851297.0 + 4043295.0, abs=1e-6)
    assert oi["n_contracts"] == 2


def test_equity_daily_return_and_range(con):
    eq = (
        con.execute(_model_sql("mart_equity_daily")).pl().filter(pl.col("symbol") == "PETR4").sort("date")
    )
    # day 2: close 10.5 vs 10.0 -> +5%
    assert eq.row(1, named=True)["daily_return_pct"] == pytest.approx(5.0, abs=1e-9)
    # 52w high tracks the running max of `high` (10.6 -> 10.8 by day 2)
    assert eq.row(1, named=True)["high_52w"] == pytest.approx(10.8, abs=1e-9)
