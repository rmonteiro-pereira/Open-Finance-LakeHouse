-- mart_trade_ohlc_1m_nrt — the streaming lane's answer to `mart_equity_daily`.
-- Same idea, four orders of magnitude faster clock: per-symbol bars with the
-- return, range and rolling measures a chart needs, computed once at build time
-- instead of by every consumer.
-- Grain: symbol x window_start.
--
-- `follows_previous` is the column a minute-grain mart needs and a daily one does
-- not. A gap between consecutive bars is normal here — it means nobody traded
-- that minute, or the capture was bounded — so `return_pct` across a gap is a
-- return over an unknown span, not a one-minute return. Flagging it lets a
-- consumer decide, rather than silently averaging incomparable numbers.
WITH base AS (
    SELECT
        symbol,
        window_start,
        window_end,
        open, high, low, close,
        volume, notional, vwap, trades, sell_volume,
        first_trade_time, last_trade_time
    FROM silver_ohlc_1m
    WHERE volume > 0          -- a zero-volume bar cannot happen upstream; guard the division anyway
),
chained AS (
    SELECT
        *,
        lag(close)        OVER w AS prev_close,
        lag(window_start) OVER w AS prev_window_start
    FROM base
    WINDOW w AS (PARTITION BY symbol ORDER BY window_start)
),
derived AS (
    SELECT
        *,
        100.0   * (close / nullif(prev_close, 0) - 1)     AS return_pct,
        -- Overnight-gap logic, at minute grain: the first trade of this bar
        -- against the last trade of the previous one.
        10000.0 * (open  / nullif(prev_close, 0) - 1)     AS gap_bps,
        10000.0 * (high - low) / nullif(vwap, 0)          AS range_bps,
        sell_volume / nullif(volume, 0)                   AS sell_share,
        prev_window_start IS NOT NULL
            AND window_start - prev_window_start = INTERVAL 1 MINUTE AS follows_previous,
        -- How much of the minute was silent: window_end minus the last trade in it.
        date_diff('second', last_trade_time, window_end)  AS quiet_tail_seconds
    FROM chained
)
SELECT
    symbol,
    window_start,
    window_end,
    open, high, low, close,
    volume, notional, vwap, trades, sell_volume,
    return_pct,
    gap_bps,
    range_bps,
    sell_share,
    follows_previous,
    quiet_tail_seconds,
    avg(vwap)                     OVER w5 AS vwap_sma_5,
    sum(volume)                   OVER w5 AS volume_5m,
    stddev_samp(return_pct)       OVER w5 AS vol_5m,
    CAST($built_at AS TIMESTAMP)          AS built_at
FROM derived
WINDOW w5 AS (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
ORDER BY symbol, window_start;
