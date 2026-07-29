-- mart_trade_latest_nrt — one row per symbol: its newest published bar, plus the
-- two freshness numbers a near-real-time tile actually needs.
-- Grain: symbol.
--
-- Freshness is stated **relative to the stream**, not to wall clock. The lane is
-- capture-bounded by design (the producer always terminates), so "seconds since
-- now" would report hours of staleness for a table that is perfectly consistent
-- with the data it was given, and would say the same thing about a genuinely
-- stalled feed. `behind_stream_seconds` compares a symbol against the newest event
-- time in the table, which is a real signal: a thinly traded symbol falls behind
-- an active one. `built_at` is recorded alongside so a consumer that *does* want
-- wall-clock age can compute it and own that interpretation.
WITH newest AS (
    SELECT max(last_trade_time) AS stream_event_time FROM silver_ohlc_1m
),
ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY symbol ORDER BY window_start DESC) AS rn,
        count(*)     OVER (PARTITION BY symbol)                            AS bars,
        sum(trades)  OVER (PARTITION BY symbol)                            AS trades_total
    FROM silver_ohlc_1m
)
SELECT
    r.symbol,
    r.window_start,
    r.window_end,
    r.open, r.high, r.low, r.close,
    r.vwap,
    r.volume,
    r.trades,
    r.last_trade_time,
    n.stream_event_time,
    date_diff('second', r.last_trade_time, n.stream_event_time) AS behind_stream_seconds,
    r.bars                                                      AS bars_published,
    r.trades_total                                              AS trades_published,
    CAST($built_at AS TIMESTAMP)                                AS built_at
FROM ranked r
CROSS JOIN newest n
WHERE r.rn = 1
ORDER BY r.symbol;
