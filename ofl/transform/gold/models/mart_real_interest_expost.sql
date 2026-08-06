-- Ex-post real interest: SELIC actually EARNED over a window, deflated by the
-- inflation actually REALISED over the same window. Both legs look backwards, and
-- both cover the same twelve months — which is what makes it ex-post.
--
-- The SELIC leg compounds the daily effective rate (`selic`, SGS 11, basis per_day),
-- so the window is only meaningful if it is complete: a missing session is silently
-- compounded away as if the market had been shut. `n_business_days_expected` comes
-- from `dim_date.is_business_day_br` and is published next to the observed count, so
-- the gap is visible in the data. `assert_expost_window_is_complete` turns it into a
-- failure rather than a footnote.
--
-- Grain: one row per ref_month, window = the twelve months ending at ref_month.
WITH bounds AS (
    SELECT
        date_trunc('month', date)::DATE AS ref_month,
        (date_trunc('month', date) - INTERVAL 11 MONTH)::DATE AS window_start,
        (date_trunc('month', date) + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE AS window_end
    FROM fact_observation
    WHERE series_id = 'ipca'
    GROUP BY 1, 2, 3
),
ipca AS (
    SELECT
        b.ref_month,
        (exp(sum(ln(1 + f.value / 100.0))) - 1) * 100 AS ipca_accum_12m_pct,
        count(*) AS n_months_observed
    FROM bounds b
    JOIN fact_observation f
      ON f.series_id = 'ipca' AND f.date BETWEEN b.window_start AND b.window_end
    GROUP BY b.ref_month
),
selic AS (
    SELECT
        b.ref_month,
        -- `selic` is a DAILY effective rate: the window value is the product of
        -- (1 + r_d), not a sum and not an average.
        (exp(sum(ln(1 + f.value / 100.0))) - 1) * 100 AS selic_accum_12m_pct,
        count(*) AS n_business_days_observed
    FROM bounds b
    JOIN fact_observation f
      ON f.series_id = 'selic' AND f.date BETWEEN b.window_start AND b.window_end
    GROUP BY b.ref_month
),
calendar AS (
    SELECT b.ref_month, count(*) AS n_business_days_expected
    FROM bounds b
    JOIN dim_date d
      ON d.date BETWEEN b.window_start AND b.window_end AND d.is_business_day_br
    GROUP BY b.ref_month
)
SELECT
    b.ref_month,
    b.window_start,
    b.window_end,
    s.selic_accum_12m_pct,
    s.n_business_days_observed,
    c.n_business_days_expected,
    i.ipca_accum_12m_pct,
    i.n_months_observed,
    ((1 + s.selic_accum_12m_pct / 100.0) / (1 + i.ipca_accum_12m_pct / 100.0) - 1) * 100
        AS real_expost_pct,
    'fisher' AS method
FROM bounds b
JOIN selic s     USING (ref_month)
JOIN ipca i      USING (ref_month)
JOIN calendar c  USING (ref_month)
WHERE i.n_months_observed = 12   -- only full twelve-month windows
ORDER BY b.ref_month;
