"""Deterministic gold-mart checks: known inputs -> hand-calculated outputs."""

from datetime import date, timedelta

import duckdb
import polars as pl
import pytest

from ofl.transform.gold.runner import GoldCheckError, _check_sql, _model_sql, execute_models
from ofl.transform.keys import instrument_id

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
            # Labels without the year, as the source publishes them — the year lives in
            # `maturity`. The old fixture baked it into the label, which is why nothing
            # here ever noticed that `(bond, date)` could not tell two maturities apart.
            "bond": ["Tesouro IPCA+", "Tesouro Prefixado"],
            "maturity": [date(2029, 8, 15), date(2027, 1, 1)],
            "date": [date(2024, 1, 2), date(2024, 1, 2)],
            "buy_rate": [5.5, 10.5],
            "sell_rate": [5.6, 10.6],
            "buy_price": [1234.56, 900.0],
            "sell_price": [1233.0, 899.0],
            "instrument_id": [
                instrument_id("tesouro", "Tesouro IPCA+", date(2029, 8, 15)),
                instrument_id("tesouro", "Tesouro Prefixado", date(2027, 1, 1)),
            ],
            "provider": ["tesouro_direto", "tesouro_direto"],
            "data_class": ["live", "live"],
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
    # Also exercises every post-build check on clean data: a violation would raise.
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
        "mart_di_curve_points",
        "mart_di_curve_slope",
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


# --- DI curve grid marts ------------------------------------------------------
#
# A hand-built DI1 strip. Day counts are engineered so key interpolation weights
# are exact decimals:
#   2y on D1: (2*365.25 - 402) / (767 - 402) = 328.5 / 365  = 0.9
#   10y on D1: (10*365.25 - 1827) / (4261 - 1827) = 1825.5 / 2434 = 0.75
D1 = date(2026, 6, 22)
D2 = D1 + timedelta(days=1)
D3 = D1 + timedelta(days=2)
# symbol -> days to expiration counted from D1
DI_DAYS = {
    "DI1U26": 146,   # ~0.40y
    "DI1F27": 256,   # ~0.70y
    "DI1N27": 402,   # ~1.10y
    "DI1N28": 767,   # ~2.10y
    "DI1N29": 1132,  # ~3.10y
    "DI1N31": 1827,  # ~5.00y
    "DI1F38": 4261,  # ~11.67y
}
# (date, symbol, settlement_rate): D1 full upward-then-humped strip; D2 a short
# 2-contract strip; D3 an inverted strip with no listings under ~1.1y.
DI_QUOTES = [
    (D1, "DI1U26", 14.9), (D1, "DI1F27", 14.5), (D1, "DI1N27", 14.0),
    (D1, "DI1N28", 13.2), (D1, "DI1N29", 12.9), (D1, "DI1N31", 12.6),
    (D1, "DI1F38", 13.8),
    (D2, "DI1U26", 14.9), (D2, "DI1N27", 14.0),
    (D3, "DI1N27", 15.0), (D3, "DI1N28", 14.4), (D3, "DI1N31", 13.5),
    (D3, "DI1F38", 13.2),
]


def _interp(lower_days: int, lower_rate: float, upper_days: int, upper_rate: float, target_years: float) -> float:
    """The linear interpolation the mart implements, re-expressed in days."""
    w = (target_years * 365.25 - lower_days) / (upper_days - lower_days)
    return lower_rate + (upper_rate - lower_rate) * w


@pytest.fixture
def di_con():
    dq = pl.DataFrame(
        {
            "symbol": [s for _, s, _ in DI_QUOTES] + ["DOLF27"],
            "date": [d for d, _, _ in DI_QUOTES] + [D1],
            "segment": ["FINANCIAL"] * (len(DI_QUOTES) + 1),
            "last_price": [r for _, _, r in DI_QUOTES] + [5000.0],
            "settlement_price": [90000.0] * len(DI_QUOTES) + [5000.0],
            # DOLF27 carries a settlement_rate on purpose: the mart must drop it
            # on the asset filter, not rely on price-quoted assets having none.
            "settlement_rate": [r for _, _, r in DI_QUOTES] + [5000.0],
            "ref_price": [None] * (len(DI_QUOTES) + 1),
            "contracts": [1000.0] * (len(DI_QUOTES) + 1),
            "notional": [1e9] * (len(DI_QUOTES) + 1),
        }
    )
    # OI for every quoted (symbol, date) except DI1F27 on D1 — that hole must
    # surface as both_legs_have_open_interest = False, not as a dropped point.
    oi_rows = [(d, s) for d, s, _ in DI_QUOTES if not (d == D1 and s == "DI1F27")]
    oi = pl.DataFrame(
        {
            "symbol": [s for _, s in oi_rows],
            "date": [d for d, _ in oi_rows],
            "asset": ["DI1"] * len(oi_rows),
            "expiration_code": [s[3:] for _, s in oi_rows],
            "segment": ["FINANCIAL"] * len(oi_rows),
            "open_interest": [100.0] * len(oi_rows),
            "open_interest_var": [0.0] * len(oi_rows),
        }
    )
    inst = pl.DataFrame(
        {
            "symbol": [*DI_DAYS.keys(), "DOLF27"],
            "asset": ["DI1"] * len(DI_DAYS) + ["DOL"],
            "expiration_date": [D1 + timedelta(days=n) for n in DI_DAYS.values()] + [D1 + timedelta(days=200)],
            "contract_multiplier": [1.0] * (len(DI_DAYS) + 1),
            "trading_ccy": ["BRL"] * (len(DI_DAYS) + 1),
        }
    )
    c = duckdb.connect()
    c.register("fact_derivatives_quote", dq.to_arrow())
    c.register("fact_open_interest", oi.to_arrow())
    c.register("dim_instrument", inst.to_arrow())
    return c


def test_di_curve_points_interpolates_on_the_grid(di_con):
    execute_models(di_con, write=False, models=["mart_di_curve_points"])
    pts = di_con.execute("SELECT * FROM mart_di_curve_points").pl()
    d1 = pts.filter(pl.col("trade_date") == D1)
    assert set(d1["tenor_label"]) == {"6m", "1y", "2y", "3y", "5y", "10y"}

    two_y = d1.filter(pl.col("tenor_label") == "2y").row(0, named=True)
    assert two_y["lower_symbol"] == "DI1N27"
    assert two_y["upper_symbol"] == "DI1N28"
    # weight (730.5 - 402) / (767 - 402) = 0.9 exactly -> 14.0 + 0.9 * (13.2 - 14.0)
    assert two_y["di_rate_pct"] == pytest.approx(13.28, abs=1e-9)

    ten_y = d1.filter(pl.col("tenor_label") == "10y").row(0, named=True)
    assert ten_y["lower_symbol"] == "DI1N31"
    assert ten_y["upper_symbol"] == "DI1F38"
    # weight 1825.5 / 2434 = 0.75 exactly -> 12.6 + 0.75 * (13.8 - 12.6)
    assert ten_y["di_rate_pct"] == pytest.approx(13.5, abs=1e-9)

    six_m = d1.filter(pl.col("tenor_label") == "6m").row(0, named=True)
    assert six_m["di_rate_pct"] == pytest.approx(_interp(146, 14.9, 256, 14.5, 0.5), abs=1e-9)
    assert six_m["max_leg_gap_years"] == pytest.approx((256 - 0.5 * 365.25) / 365.25, abs=1e-9)

    # The price-quoted DOL contract must never appear as a leg.
    assert "DOLF27" not in set(pts["lower_symbol"]) | set(pts["upper_symbol"])


def test_di_curve_points_never_extrapolate(di_con):
    execute_models(di_con, write=False, models=["mart_di_curve_points"])
    pts = di_con.execute("SELECT * FROM mart_di_curve_points").pl()
    # D2's strip is 0.40y-1.10y: it straddles only 6m and 1y — no long points invented.
    assert set(pts.filter(pl.col("trade_date") == D2)["tenor_label"]) == {"6m", "1y"}
    # D3's shortest listing is ~1.1y: no 6m/1y points invented at the short end.
    assert set(pts.filter(pl.col("trade_date") == D3)["tenor_label"]) == {"2y", "3y", "5y", "10y"}


def test_di_curve_points_open_interest_flag(di_con):
    execute_models(di_con, write=False, models=["mart_di_curve_points"])
    d1 = di_con.execute("SELECT * FROM mart_di_curve_points").pl().filter(pl.col("trade_date") == D1)
    flags = {r["tenor_label"]: r["both_legs_have_open_interest"] for r in d1.iter_rows(named=True)}
    # DI1F27 (leg of 6m and 1y on D1) has no OI row -> coalesce(_, 0) > 0 is False.
    assert flags["6m"] is False
    assert flags["1y"] is False
    assert flags["2y"] is True


def test_di_curve_slope_pivot_and_shape(di_con):
    execute_models(di_con, write=False, models=["mart_di_curve_points", "mart_di_curve_slope"])
    slope = di_con.execute("SELECT * FROM mart_di_curve_slope ORDER BY trade_date").pl()
    assert slope.height == 3

    d1 = slope.row(0, named=True)
    assert d1["n_curve_points"] == 6
    # 2s10s = 13.5 - 13.28 = +0.22 > +0.10 -> normal
    assert d1["slope_2s10s_pp"] == pytest.approx(0.22, abs=1e-9)
    assert d1["curve_shape"] == "normal"
    assert d1["rate_6m_pct"] == pytest.approx(_interp(146, 14.9, 256, 14.5, 0.5), abs=1e-9)

    d2 = slope.row(1, named=True)
    assert d2["n_curve_points"] == 2
    # No 2y/10y points on the short strip: slopes and shape stay null, never invented.
    assert d2["slope_2s10s_pp"] is None
    assert d2["curve_shape"] is None

    d3 = slope.row(2, named=True)
    assert d3["n_curve_points"] == 4
    expected = _interp(1825, 13.5, 4259, 13.2, 10.0) - _interp(400, 15.0, 765, 14.4, 2.0)
    assert d3["slope_2s10s_pp"] == pytest.approx(expected, abs=1e-9)
    assert d3["curve_shape"] == "inverted"


# --- post-build checks: each one must be able to fail -------------------------


def test_bracketing_check_catches_bad_interpolation():
    """A flipped interpolation weight or a wrong-side leg must return violations."""
    c = duckdb.connect()
    c.execute(
        """
        CREATE VIEW mart_di_curve_points AS
        SELECT * FROM (VALUES
            -- healthy: rate inside its legs, legs straddle the target
            ('2026-06-22|1y', DATE '2026-06-22', '1y', 1.0, 0.70, 1.10, 14.5, 14.0, 14.13),
            -- flipped-weight bug: interpolated rate lands OUTSIDE the two legs
            ('2026-06-22|2y', DATE '2026-06-22', '2y', 2.0, 1.10, 2.10, 14.0, 13.2, 14.72),
            -- extrapolation bug: 'lower' leg sits past the grid tenor
            ('2026-06-22|3y', DATE '2026-06-22', '3y', 3.0, 3.10, 5.00, 12.9, 12.6, 12.85)
        ) AS t(curve_point_key, trade_date, tenor_label, target_tenor_years,
               lower_tenor_years, upper_tenor_years, lower_rate_pct, upper_rate_pct, di_rate_pct)
        """
    )
    viol = c.execute(_check_sql("assert_di_curve_points_are_bracketed")).pl()
    reasons = dict(zip(viol["tenor_label"], viol["failure_reason"], strict=True))
    assert reasons == {"2y": "interpolated_rate_outside_legs", "3y": "lower_leg_past_target"}


def test_dense_month_check_catches_gap_and_fails_the_run():
    """One missing month in the macro panel must fail the gold run loudly."""
    months = [date(2024, m, 1) for m in range(1, 13) if m != 6]
    fact = pl.DataFrame(
        [("selic_meta", d, 13.0) for d in months],
        schema=["series_id", "date", "value"],
        orient="row",
    )
    c = duckdb.connect()
    c.register("fact_observation", fact.to_arrow())
    # skip_on_error=True is the orchestrated path: a check breach must NOT be
    # swallowed the way a missing upstream is.
    with pytest.raises(GoldCheckError, match="assert_macro_panel_has_no_month_gaps"):
        execute_models(c, write=False, models=["mart_macro_dashboard"], skip_on_error=True)
    row = c.execute(_check_sql("assert_macro_panel_has_no_month_gaps")).pl().row(0, named=True)
    assert row["observed_months"] == 11
    assert row["expected_months"] == 12


def test_ipca_check_catches_row_window_spanning_13_months():
    """The failure mode the dbt lane guarded: `ROWS BETWEEN 11 PRECEDING` counts
    rows, so a missing IPCA month makes the mart publish a "12-month" figure that
    actually spans 13 calendar months. The panel stays dense (SELIC covers the
    month), so only the calendar-window recomputation can catch it."""
    months = [(2024, m) for m in range(1, 13)] + [(2025, 1)]
    rows = [("ipca", date(y, m, 1), 1.0) for y, m in months if (y, m) != (2024, 6)]
    rows += [("selic_meta", date(y, m, 1), 13.0) for y, m in months]
    fact = pl.DataFrame(rows, schema=["series_id", "date", "value"], orient="row")
    c = duckdb.connect()
    c.register("fact_observation", fact.to_arrow())
    with pytest.raises(GoldCheckError, match="assert_real_interest_ipca_recomputes"):
        execute_models(c, write=False, models=["mart_real_interest"], skip_on_error=True)
    viol = c.execute(_check_sql("assert_real_interest_ipca_recomputes")).pl()
    # 2025-01 is the corrupt row: its 12-row window spans 2024-01..2025-01 minus June.
    assert viol.height == 1
    assert viol.row(0, named=True)["month"] == date(2025, 1, 1)
    assert viol.row(0, named=True)["failure_reason"] == "mart_month_without_full_calendar_window"
    # The dense-grid check alone would NOT have caught this: SELIC keeps the panel dense.
    execute_models(c, write=False, models=["mart_macro_dashboard"])


def test_ipca_check_catches_value_drift(con):
    """The reconciliation itself must be able to fail: nudge the published number
    by 1e-6 p.p. (far above the 1e-9 tolerance) and every month must be flagged."""
    mart = con.execute(_model_sql("mart_real_interest")).pl()
    tampered = mart.with_columns((pl.col("ipca_accum_12m") + 1e-6).alias("ipca_accum_12m"))
    con.register("mart_real_interest", tampered.to_arrow())
    viol = con.execute(_check_sql("assert_real_interest_ipca_recomputes")).pl()
    assert viol.height == mart.height > 0
    assert set(viol["failure_reason"]) == {"value_mismatch"}


def test_yield_curve_separates_real_prices_from_sandbox_prices():
    """Defect B, as an assertion.

    `fact_treasury` receives Tesouro (real, ODbL) and ANBIMA through the same MERGE, and
    the ANBIMA series currently ingested run against a sandbox whose values are
    fictitious by the provider's own description. The mart used to project no
    discriminator at all: the two were separable only as a side effect of a
    `bond ILIKE '%IPCA%'` bucket, which is a presentation rule and would classify an
    ANBIMA SELIC code as 'other' rather than as 'not real'.
    """
    c = duckdb.connect()
    mixed = pl.DataFrame(
        {
            "instrument_id": ["i-real", "i-fake"],
            "bond": ["Tesouro IPCA+", "760199"],
            "maturity": [date(2035, 5, 15), date(2035, 5, 15)],
            "date": [date(2026, 8, 4)] * 2,
            "buy_rate": [7.10, 7.99],
            "sell_rate": [7.12, 8.01],
            "buy_price": [3000.0, 3000.0],
            "sell_price": [2999.0, 2999.0],
            "provider": ["tesouro_direto", "anbima"],
            "data_class": ["live", "sandbox"],
        }
    )
    c.register("fact_treasury", mixed.to_arrow())
    out = c.execute(_model_sql("mart_yield_curve")).pl()

    assert {"provider", "data_class", "instrument_id"} <= set(out.columns)
    live = out.filter(pl.col("data_class") == "live")
    assert live.height == 1 and live.row(0, named=True)["provider"] == "tesouro_direto"
    # Both rows survive — the mart must not hide the sandbox row, it must LABEL it, so
    # the release gate can drop it for a stated reason instead of it vanishing untraced.
    assert out.height == 2
