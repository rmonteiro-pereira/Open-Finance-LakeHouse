"""M5: the near-real-time DuckDB mart — known bars in, hand-calculated numbers out.

Same approach as ``tests/test_gold_marts.py``: build a tiny silver table by hand,
run the real SQL models against it, and assert on values worked out on paper. The
fixture deliberately contains a **gap** (no bar at 00:03) because that is the case
a minute-grain mart has to handle and a daily one never sees.
"""

from datetime import datetime, timedelta

import duckdb
import pytest

from ofl.streaming.mart import MODELS, SOURCE_VIEW, build_nrt_mart, execute_models, model_sql

BUILT_AT = datetime(2026, 7, 29, 5, 0, 0)


def bar(minute: int, symbol: str, *, open_, high, low, close, volume, notional, sell, trades, tail=0):
    """One silver row. ``tail`` is how many seconds before window_end it ended."""
    start = datetime(2026, 7, 29, 0, minute)
    end = start + timedelta(minutes=1)
    return (
        start,
        end,
        symbol,
        open_,
        high,
        low,
        close,
        volume,
        notional,
        notional / volume,
        trades,
        sell,
        start,
        end - timedelta(seconds=tail),
    )


# BTC: 00:00, 00:01, 00:02, then a gap, then 00:04. ETH: two bars, ending earlier
# than BTC's newest so the freshness column has something to report.
ROWS = [
    bar(0, "BTC", open_=100.0, high=110.0, low=90.0, close=100.0, volume=10.0, notional=1000.0, sell=2.0, trades=5),
    bar(1, "BTC", open_=101.0, high=104.0, low=100.0, close=102.0, volume=20.0, notional=2040.0, sell=10.0, trades=7),
    bar(2, "BTC", open_=102.0, high=103.0, low=101.0, close=103.02, volume=5.0, notional=510.0, sell=0.0, trades=3),
    bar(4, "BTC", open_=110.0, high=110.0, low=110.0, close=110.0, volume=1.0, notional=110.0, sell=1.0, trades=1, tail=10),
    bar(0, "ETH", open_=10.0, high=10.0, low=10.0, close=10.0, volume=100.0, notional=1000.0, sell=50.0, trades=4),
    bar(1, "ETH", open_=10.0, high=12.0, low=8.0, close=11.0, volume=50.0, notional=500.0, sell=25.0, trades=6, tail=30),
]

SCHEMA = """
    window_start TIMESTAMP, window_end TIMESTAMP, symbol VARCHAR,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume DOUBLE, notional DOUBLE, vwap DOUBLE, trades BIGINT, sell_volume DOUBLE,
    first_trade_time TIMESTAMP, last_trade_time TIMESTAMP
"""


@pytest.fixture
def con():
    con = duckdb.connect()
    con.execute(f"CREATE TABLE {SOURCE_VIEW} ({SCHEMA})")
    con.executemany(f"INSERT INTO {SOURCE_VIEW} VALUES ({', '.join(['?'] * 14)})", ROWS)
    execute_models(con, built_at=BUILT_AT)
    return con


def one(con, sql: str):
    return con.execute(sql).fetchone()


# --- the models run at all --------------------------------------------------


def test_every_model_materialises_a_table(con):
    for name in MODELS:
        assert one(con, f"SELECT count(*) FROM {name}")[0] > 0


def test_the_bar_mart_keeps_the_silver_grain(con):
    assert one(con, "SELECT count(*), count(DISTINCT (symbol, window_start)) FROM mart_trade_ohlc_1m_nrt") == (6, 6)


def test_the_latest_mart_is_one_row_per_symbol(con):
    rows = con.execute("SELECT symbol, window_start FROM mart_trade_latest_nrt ORDER BY symbol").fetchall()
    assert rows == [
        ("BTC", datetime(2026, 7, 29, 0, 4)),
        ("ETH", datetime(2026, 7, 29, 0, 1)),
    ]


# --- the derived columns, on paper ------------------------------------------


def test_return_is_close_over_the_previous_close(con):
    # BTC 00:01 closed 102.00 against 100.00 -> +2%.
    got = one(con, "SELECT return_pct FROM mart_trade_ohlc_1m_nrt WHERE symbol='BTC' AND window_start='2026-07-29 00:01'")
    assert got[0] == pytest.approx(2.0)


def test_the_first_bar_of_a_symbol_has_no_return(con):
    got = one(con, "SELECT return_pct, gap_bps FROM mart_trade_ohlc_1m_nrt WHERE symbol='BTC' AND window_start='2026-07-29 00:00'")
    assert got == (None, None)


def test_gap_is_this_bars_open_against_the_previous_close(con):
    # BTC 00:01 opened 101.00 after closing 100.00 -> +100 bps.
    got = one(con, "SELECT gap_bps FROM mart_trade_ohlc_1m_nrt WHERE symbol='BTC' AND window_start='2026-07-29 00:01'")
    assert got[0] == pytest.approx(100.0)


def test_range_is_high_minus_low_over_vwap_in_bps(con):
    # ETH 00:01: (12 - 8) / 10 = 0.4 -> 4000 bps.
    got = one(con, "SELECT range_bps FROM mart_trade_ohlc_1m_nrt WHERE symbol='ETH' AND window_start='2026-07-29 00:01'")
    assert got[0] == pytest.approx(4000.0)


def test_sell_share_is_the_fraction_that_hit_the_bid(con):
    got = con.execute(
        "SELECT sell_share FROM mart_trade_ohlc_1m_nrt WHERE symbol='BTC' ORDER BY window_start"
    ).fetchall()
    assert [r[0] for r in got] == pytest.approx([0.2, 0.5, 0.0, 1.0])


def test_quiet_tail_measures_silence_at_the_end_of_the_minute(con):
    got = one(con, "SELECT quiet_tail_seconds FROM mart_trade_ohlc_1m_nrt WHERE symbol='ETH' AND window_start='2026-07-29 00:01'")
    assert got[0] == 30


# --- the gap, which is the whole reason this mart differs from a daily one ---


def test_a_bar_after_a_missing_minute_is_flagged(con):
    got = con.execute(
        "SELECT window_start, follows_previous FROM mart_trade_ohlc_1m_nrt WHERE symbol='BTC' ORDER BY window_start"
    ).fetchall()
    # 00:00 has no predecessor at all; 00:04 has one, but three minutes back.
    assert [r[1] for r in got] == [False, True, True, False]


def test_the_return_across_the_gap_is_still_computed_but_marked(con):
    # It is not null — a consumer may well want it — but it spans four minutes,
    # not one, and `follows_previous` is how they find that out.
    row = one(
        con,
        "SELECT return_pct, follows_previous FROM mart_trade_ohlc_1m_nrt "
        "WHERE symbol='BTC' AND window_start='2026-07-29 00:04'",
    )
    assert row[0] is not None and row[1] is False


# --- rolling windows --------------------------------------------------------


def test_the_rolling_window_is_five_bars_and_per_symbol(con):
    # BTC vwaps are 100, 102, 102, 110; the 4-bar average is 103.5. ETH's bars must
    # not leak in, and the window is short enough here that it spans everything.
    got = one(con, "SELECT vwap_sma_5, volume_5m FROM mart_trade_ohlc_1m_nrt WHERE symbol='BTC' AND window_start='2026-07-29 00:04'")
    assert got[0] == pytest.approx(103.5)
    assert got[1] == pytest.approx(36.0)


def test_the_rolling_average_of_the_first_bar_is_itself(con):
    got = one(con, "SELECT vwap_sma_5 FROM mart_trade_ohlc_1m_nrt WHERE symbol='ETH' AND window_start='2026-07-29 00:00'")
    assert got[0] == pytest.approx(10.0)


# --- freshness --------------------------------------------------------------


def test_freshness_is_measured_against_the_newest_event_in_the_table(con):
    # Newest last_trade_time anywhere is BTC's 00:04:50. ETH's newest bar ended its
    # trading at 00:01:30, so ETH is 200 seconds behind the stream and BTC is 0.
    rows = dict(
        con.execute("SELECT symbol, behind_stream_seconds FROM mart_trade_latest_nrt").fetchall()
    )
    assert rows == {"BTC": 0, "ETH": 200}


def test_the_latest_mart_carries_the_per_symbol_totals(con):
    rows = dict(con.execute("SELECT symbol, trades_published FROM mart_trade_latest_nrt").fetchall())
    assert rows == {"BTC": 16, "ETH": 10}
    assert dict(con.execute("SELECT symbol, bars_published FROM mart_trade_latest_nrt").fetchall()) == {
        "BTC": 4,
        "ETH": 2,
    }


def test_both_models_stamp_the_same_build_time(con):
    for name in MODELS:
        assert one(con, f"SELECT DISTINCT built_at FROM {name}") == (BUILT_AT,)


# --- wiring -----------------------------------------------------------------


def test_a_missing_silver_table_is_an_error_not_an_empty_mart(tmp_path):
    # An empty DuckDB file that opens cleanly is the worst possible failure mode.
    with pytest.raises(FileNotFoundError):
        build_nrt_mart(tmp_path / "nrt.duckdb", source=tmp_path / "no-such-silver")


def test_every_declared_model_has_a_sql_file():
    for name in MODELS:
        assert f"-- {name}" in model_sql(name)


def test_the_models_read_only_the_silver_view():
    # No cross-mart references: each model is rebuildable on its own, which is what
    # lets the whole mart be recomputed every pass without an ordering constraint.
    for name in MODELS:
        for other in MODELS:
            assert other not in model_sql(name).replace(f"-- {name}", "")
        assert SOURCE_VIEW in model_sql(name)
