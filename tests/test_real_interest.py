"""Ex-ante and ex-post real interest — the two things `mart_real_interest` was not.

The shipped mart divides the SELIC target (forward-looking) by trailing realised IPCA
(backward-looking) and calls itself ex-post. These tests pin the two honest marts against
hand-calculated numbers, and pin the two traps that would quietly reintroduce the defect:
collapsing a Copom month with `avg`, and compounding an incomplete window.
"""

from datetime import date

import duckdb
import polars as pl
import pytest

from ofl.calendar import build_calendar
from ofl.transform.gold.runner import GoldCheckError, _check_sql, _model_sql, execute_models

MONTHS = [(2025, m) for m in range(1, 13)] + [(2026, 1)]


def _con(selic_meta_rows, focus_rows, *, selic_daily=None, ipca_pct=0.5, drop_days=0):
    """A connection carrying just what the two marts read."""
    rows: list[tuple] = []
    for y, m in MONTHS:
        rows.append(("ipca", date(y, m, 1), ipca_pct))
    rows += [("selic_meta", d, v) for d, v in selic_meta_rows]
    rows += [("focus_ipca_12m", d, v) for d, v in focus_rows]

    cal = build_calendar("2024-12-01", "2026-02-28")
    if selic_daily is not None:
        rows += selic_daily
    else:
        # Covers BOTH twelve-month windows the ipca fixture produces (2025-12 and
        # 2026-01); a shorter range would fail the completeness gate for the right
        # reason on the wrong window, and the test would pass without meaning it.
        business = cal.filter(pl.col("is_business_day_br"))["date"].to_list()
        window = [d for d in business if date(2025, 1, 1) <= d <= date(2026, 1, 31)]
        if drop_days:
            window = window[:-drop_days]  # drops from the END: only the last window
        rows += [("selic", d, 0.05) for d in window]

    fact = pl.DataFrame(rows, schema=["series_id", "date", "value"], orient="row")
    c = duckdb.connect()
    c.register("fact_observation", fact.to_arrow())
    c.register("dim_date", cal.to_arrow())
    return c


# ------------------------------------------------------------------------- ex-ante


def test_exante_uses_the_rate_in_force_not_the_month_average():
    """The Copom trap.

    A cut from 15.00 to 14.50 mid-month averages to a rate that was in force on no day
    of that month. `mart_real_interest` collapses its month with `avg(value)`; replicating
    that here would publish a policy rate that never existed.
    """
    c = _con(
        selic_meta_rows=[(date(2025, 5, 1), 15.0), (date(2025, 5, 18), 14.5)],
        focus_rows=[(date(2025, 5, 26), 4.0)],
    )
    out = c.execute(_model_sql("mart_real_interest_exante")).pl()
    row = out.filter(pl.col("ref_month") == date(2025, 5, 1)).row(0, named=True)

    assert row["selic_target_pct_pa"] == 14.5
    assert row["selic_asof_date"] == date(2025, 5, 18)
    assert row["selic_target_pct_pa"] != pytest.approx((15.0 + 14.5) / 2)


def test_exante_is_fisher_exact_not_subtraction():
    c = _con(
        selic_meta_rows=[(date(2025, 5, 1), 15.0)],
        focus_rows=[(date(2025, 5, 26), 4.0)],
    )
    row = (
        c.execute(_model_sql("mart_real_interest_exante"))
        .pl()
        .filter(pl.col("ref_month") == date(2025, 5, 1))
        .row(0, named=True)
    )
    fisher = ((1 + 15.0 / 100) / (1 + 4.0 / 100) - 1) * 100
    assert row["real_exante_pct_pa"] == pytest.approx(fisher, abs=1e-9)
    # 10.577 vs 11.000 — the shorthand is off by 42 bps at these levels.
    assert row["real_exante_pct_pa"] != pytest.approx(15.0 - 4.0, abs=1e-3)
    assert row["method"] == "fisher"


def test_exante_takes_the_last_survey_not_a_later_one():
    """The expectation must be the one available at month end, never a later revision —
    otherwise the number is only reproducible by someone who has the future."""
    c = _con(
        selic_meta_rows=[(date(2025, 5, 1), 15.0)],
        focus_rows=[(date(2025, 5, 26), 4.0), (date(2025, 6, 9), 4.6)],
    )
    row = (
        c.execute(_model_sql("mart_real_interest_exante"))
        .pl()
        .filter(pl.col("ref_month") == date(2025, 5, 1))
        .row(0, named=True)
    )
    assert row["focus_survey_date"] == date(2025, 5, 26)
    assert row["ipca_exp_12m_pct"] == 4.0


def test_exante_grain_is_one_row_per_month():
    c = _con(
        selic_meta_rows=[(date(2025, 5, 1), 15.0), (date(2025, 5, 18), 14.5)],
        focus_rows=[(date(2025, 5, 5), 4.0), (date(2025, 5, 26), 4.1)],
    )
    out = c.execute(_model_sql("mart_real_interest_exante")).pl()
    assert out.height == out["ref_month"].n_unique()


# -------------------------------------------------------------------------- ex-post


def test_expost_compounds_the_daily_rate_and_publishes_its_inputs():
    c = _con(selic_meta_rows=[(date(2025, 5, 1), 15.0)], focus_rows=[(date(2025, 5, 26), 4.0)])
    out = c.execute(_model_sql("mart_real_interest_expost")).pl()
    row = out.filter(pl.col("ref_month") == date(2026, 1, 1)).row(0, named=True)

    n = row["n_business_days_observed"]
    expected_selic = ((1.0005**n) - 1) * 100
    expected_ipca = ((1.005**12) - 1) * 100
    assert row["selic_accum_12m_pct"] == pytest.approx(expected_selic, abs=1e-9)
    assert row["ipca_accum_12m_pct"] == pytest.approx(expected_ipca, abs=1e-9)
    assert row["real_expost_pct"] == pytest.approx(
        ((1 + expected_selic / 100) / (1 + expected_ipca / 100) - 1) * 100, abs=1e-9
    )
    assert row["window_start"] == date(2025, 2, 1)
    assert row["window_end"] == date(2026, 1, 31)


def test_expost_window_check_passes_on_a_complete_window():
    c = _con(selic_meta_rows=[(date(2025, 5, 1), 15.0)], focus_rows=[(date(2025, 5, 26), 4.0)])
    execute_models(c, write=False, models=["mart_real_interest_expost"])


def test_expost_window_check_fires_on_a_missing_session():
    """A missing session is compounded away as if the market had been shut. The number
    stays plausible, which is precisely why it needs a gate rather than a reader's eye."""
    c = _con(
        selic_meta_rows=[(date(2025, 5, 1), 15.0)],
        focus_rows=[(date(2025, 5, 26), 4.0)],
        drop_days=3,
    )
    with pytest.raises(GoldCheckError, match="assert_expost_window_is_complete"):
        execute_models(c, write=False, models=["mart_real_interest_expost"], skip_on_error=True)

    viol = c.execute(_check_sql("assert_expost_window_is_complete")).pl()
    assert viol.height >= 1
    assert set(viol["failure_reason"]) == {"incomplete_business_day_window"}
    assert viol["missing_sessions"].max() == 3


def test_expost_expected_sessions_come_from_the_brazilian_calendar():
    """Not from `date_diff`, and not from weekdays: holidays are sessions that never
    existed, and counting them as missing would fire the gate on a healthy window."""
    c = _con(selic_meta_rows=[(date(2025, 5, 1), 15.0)], focus_rows=[(date(2025, 5, 26), 4.0)])
    row = (
        c.execute(_model_sql("mart_real_interest_expost"))
        .pl()
        .filter(pl.col("ref_month") == date(2026, 1, 1))
        .row(0, named=True)
    )
    cal = build_calendar("2025-02-01", "2026-01-31")
    assert row["n_business_days_expected"] == int(cal["is_business_day_br"].sum())
    weekdays = cal.filter(pl.col("date").dt.weekday() <= 5).height
    assert row["n_business_days_expected"] < weekdays  # holidays actually removed some


def test_the_deprecated_mart_is_still_published():
    """The deprecation window is the point: a consumer with no replacement staged must
    not lose its table in the same release that announces the successor."""
    from ofl.transform.gold.runner import MODELS

    assert "mart_real_interest" in MODELS
    assert "mart_real_interest_exante" in MODELS
    assert "mart_real_interest_expost" in MODELS
