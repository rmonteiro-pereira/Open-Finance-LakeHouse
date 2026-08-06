-- Ex-ante real interest: the policy rate in force, against the inflation the market
-- expects — both read at the same instant, which is what makes it ex-ante.
--
-- `mart_real_interest` (still published, deprecated) divides the SELIC TARGET, a
-- forward-looking policy rate, by TRAILING realised 12-month IPCA. That is neither
-- ex-ante nor ex-post: the numerator looks forward and the denominator looks back.
--
-- Two rules this file exists to enforce:
--
--   * The month's rate is the one IN FORCE at month end, never the month's average.
--     In a Copom month `avg` returns a rate that was never in force on any day: a
--     cut from 15.00 to 14.50 on the 18th averages to ~14.79%.
--   * Fisher exact, never the subtraction shorthand. At two-digit Brazilian rates the
--     two conventions diverge materially, and publishing without fixing one is
--     publishing an ambiguity.
--
-- Grain: one row per ref_month. Every input is carried alongside the result so the
-- number can be audited by whoever receives it, without reaching back to the source.
WITH month_end AS (
    SELECT
        date_trunc('month', date)::DATE AS ref_month,
        (date_trunc('month', date) + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE AS eom
    FROM fact_observation
    WHERE series_id = 'selic_meta'
    GROUP BY 1, 2
),
selic AS (
    SELECT
        m.ref_month,
        m.eom,
        f.date  AS selic_asof_date,
        f.value AS selic_target_pct_pa
    FROM month_end m
    JOIN fact_observation f
      ON f.series_id = 'selic_meta' AND f.date <= m.eom
    QUALIFY row_number() OVER (PARTITION BY m.ref_month ORDER BY f.date DESC) = 1
),
focus AS (
    SELECT
        m.ref_month,
        f.date  AS focus_survey_date,
        f.value AS ipca_exp_12m_pct
    FROM month_end m
    JOIN fact_observation f
      ON f.series_id = 'focus_ipca_12m' AND f.date <= m.eom
    QUALIFY row_number() OVER (PARTITION BY m.ref_month ORDER BY f.date DESC) = 1
)
SELECT
    s.ref_month,
    s.selic_asof_date,
    s.selic_target_pct_pa,
    o.focus_survey_date,
    o.ipca_exp_12m_pct,
    ((1 + s.selic_target_pct_pa / 100.0) / (1 + o.ipca_exp_12m_pct / 100.0) - 1) * 100
        AS real_exante_pct_pa,
    'fisher' AS method
FROM selic s
JOIN focus o USING (ref_month)
ORDER BY s.ref_month;
