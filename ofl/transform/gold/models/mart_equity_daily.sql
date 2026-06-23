-- mart_equity_daily — per-security daily analytics over the conformed
-- security_price fact (Yahoo families, b3 index, B3 COTAHIST).
-- Grain: symbol x date. Daily return, 21d SMA + realized vol, 52-week range.
WITH base AS (
    SELECT symbol, date, open, high, low, close, volume
    FROM fact_security_price
    WHERE close IS NOT NULL
),
ret AS (
    SELECT
        *,
        100.0 * (close / NULLIF(lag(close) OVER (PARTITION BY symbol ORDER BY date), 0) - 1)
            AS daily_return_pct
    FROM base
)
SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    daily_return_pct,
    avg(close)                  OVER w21  AS sma_21,
    stddev_samp(daily_return_pct) OVER w21  AS vol_21d,
    max(high)                   OVER w252 AS high_52w,
    min(low)                    OVER w252 AS low_52w
FROM ret
WINDOW
    w21  AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20  PRECEDING AND CURRENT ROW),
    w252 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
ORDER BY symbol, date;
