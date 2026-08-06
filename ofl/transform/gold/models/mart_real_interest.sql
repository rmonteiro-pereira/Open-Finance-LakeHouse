-- DEPRECATED — superseded by `mart_real_interest_exante` and `mart_real_interest_expost`.
--
-- This mart is NOT ex-post, despite what its header said for its whole life. It divides
-- the Copom SELIC TARGET for the month — a forward-looking policy rate — by the IPCA
-- accumulated over the twelve months that already happened. The numerator looks forward
-- and the denominator looks back, so the result is neither of the two things a reader
-- would assume it is. It is the loose "juro real" of newspaper copy.
--
-- It keeps its name and keeps being published through the deprecation window: renaming
-- it is a MAJOR break for a consumer who has no replacement staged yet, and the window
-- exists so that consumer can migrate on its own schedule. Removal is a separate,
-- announced release.
--
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
