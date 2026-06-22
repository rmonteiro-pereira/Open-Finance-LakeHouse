-- FX mart: USD/BRL & EUR/BRL daily levels, daily returns, 21-day realized vol, MTD return.
WITH fx AS (
    SELECT series_id, date, value
    FROM fact_observation
    WHERE series_id IN ('usd_brl', 'eur_brl')
),
ret AS (
    SELECT
        series_id,
        date,
        value,
        (value / lag(value) OVER (PARTITION BY series_id ORDER BY date) - 1) * 100 AS daily_return_pct
    FROM fx
)
SELECT
    series_id,
    date,
    value AS rate,
    daily_return_pct,
    stddev_samp(daily_return_pct)
        OVER (PARTITION BY series_id ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS vol_21d,
    (value / first_value(value)
        OVER (PARTITION BY series_id, date_trunc('month', date) ORDER BY date) - 1) * 100 AS mtd_return_pct
FROM ret
ORDER BY series_id, date;
