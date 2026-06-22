-- Inflation panel: every price index side-by-side, monthly and 12-month accumulated.
WITH monthly AS (
    SELECT series_id, date_trunc('month', date)::DATE AS month, value AS mom
    FROM fact_observation
    WHERE series_id IN ('ipca', 'ipca_15', 'inpc', 'igp_m', 'igp_di', 'igp_10')
),
accum AS (
    SELECT
        series_id,
        month,
        mom,
        (exp(sum(ln(1 + mom / 100.0))
             OVER (PARTITION BY series_id ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)) - 1) * 100 AS accum_12m
    FROM monthly
)
SELECT
    month,
    max(mom)       FILTER (WHERE series_id = 'ipca')    AS ipca_mom,
    max(accum_12m) FILTER (WHERE series_id = 'ipca')    AS ipca_12m,
    max(mom)       FILTER (WHERE series_id = 'ipca_15') AS ipca15_mom,
    max(mom)       FILTER (WHERE series_id = 'inpc')    AS inpc_mom,
    max(accum_12m) FILTER (WHERE series_id = 'inpc')    AS inpc_12m,
    max(mom)       FILTER (WHERE series_id = 'igp_m')   AS igpm_mom,
    max(accum_12m) FILTER (WHERE series_id = 'igp_m')   AS igpm_12m,
    max(mom)       FILTER (WHERE series_id = 'igp_di')  AS igpdi_mom,
    max(mom)       FILTER (WHERE series_id = 'igp_10')  AS igp10_mom
FROM accum
GROUP BY month
ORDER BY month;
