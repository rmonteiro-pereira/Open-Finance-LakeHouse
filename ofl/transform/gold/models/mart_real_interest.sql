-- Ex-post real interest rate: Copom SELIC target deflated by 12-month accumulated IPCA.
-- real = ((1 + selic/100) / (1 + ipca_12m/100) - 1) * 100
WITH ipca_m AS (
    SELECT date_trunc('month', date)::DATE AS month, value AS ipca_mom
    FROM fact_observation
    WHERE series_id = 'ipca'
),
ipca_12m AS (
    SELECT
        month,
        ipca_mom,
        (exp(sum(ln(1 + ipca_mom / 100.0))
             OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)) - 1) * 100 AS ipca_accum_12m,
        count(*) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS months_in_window
    FROM ipca_m
),
selic AS (
    SELECT date_trunc('month', date)::DATE AS month, avg(value) AS selic_target
    FROM fact_observation
    WHERE series_id = 'selic_meta'
    GROUP BY 1
)
SELECT
    s.month,
    s.selic_target,
    i.ipca_accum_12m,
    ((1 + s.selic_target / 100.0) / (1 + i.ipca_accum_12m / 100.0) - 1) * 100 AS real_interest_rate
FROM selic s
JOIN ipca_12m i USING (month)
WHERE i.months_in_window = 12          -- only full 12-month windows
ORDER BY s.month;
