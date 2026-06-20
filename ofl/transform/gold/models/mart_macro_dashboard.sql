-- Wide monthly macro panel for BI: policy rate, inflation, FX, debt/GDP.
-- Month-end value per series (last observation in the month).
WITH ranked AS (
    SELECT
        series_id,
        date_trunc('month', date)::DATE AS month,
        value,
        row_number() OVER (PARTITION BY series_id, date_trunc('month', date) ORDER BY date DESC) AS rn
    FROM fact_observation
    WHERE series_id IN ('selic_meta', 'ipca', 'usd_brl', 'divida_pib')
),
month_end AS (
    SELECT series_id, month, value FROM ranked WHERE rn = 1
)
SELECT
    month,
    max(value) FILTER (WHERE series_id = 'selic_meta') AS selic_target,
    max(value) FILTER (WHERE series_id = 'ipca')       AS ipca_mom,
    max(value) FILTER (WHERE series_id = 'usd_brl')    AS usd_brl,
    max(value) FILTER (WHERE series_id = 'divida_pib') AS debt_to_gdp_pct
FROM month_end
GROUP BY month
ORDER BY month;
