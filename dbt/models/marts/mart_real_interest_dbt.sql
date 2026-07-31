-- Ex-post real interest rate, re-expressed as a dbt model.
--
-- This is the dbt lane's counterpart to `ofl/transform/gold/models/mart_real_interest.sql`.
-- The transformation logic is the same in both lanes:
--   1. compound IPCA month-over-month over a trailing 12-month window
--        ipca_12m = (exp(sum(ln(1 + ipca_mom/100))) - 1) * 100
--   2. deflate the SELIC target by it (Fisher)
--        real     = ((1 + selic/100) / (1 + ipca_12m/100) - 1) * 100
--   3. keep only months whose 12-month window is complete.
--
-- One input differs, and the difference is real, not cosmetic. The production
-- mart reads `fact_observation` directly and averages every SELIC-target
-- observation inside the month; this lane's only available SELIC input is the
-- exported macro panel, which already collapsed each month to its *last*
-- observation. In months containing a Copom decision the two disagree — by up
-- to ~4.9 p.p. on the policy rate over the history in the export. The IPCA leg,
-- which is what the compounding logic actually exercises, is identical to the
-- production mart to floating-point precision; `tests/assert_real_interest_ipca_matches_gold_mart.sql`
-- asserts exactly that on every month.

WITH ipca_monthly AS (

    SELECT
        month,
        ipca_mom_pct
    FROM {{ ref('stg_gold__macro_monthly') }}
    WHERE ipca_mom_pct IS NOT NULL

),

ipca_compounded AS (

    SELECT
        month,
        ipca_mom_pct,
        (exp(sum(ln(1 + ipca_mom_pct / 100.0))
             OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)) - 1) * 100
            AS ipca_accum_12m_pct,
        count(*) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
            AS months_in_window
    FROM ipca_monthly

),

policy_rate AS (

    SELECT
        month,
        selic_target_pct
    FROM {{ ref('stg_gold__macro_monthly') }}
    WHERE selic_target_pct IS NOT NULL

)

SELECT
    p.month,
    p.selic_target_pct,
    i.ipca_mom_pct,
    i.ipca_accum_12m_pct,
    ((1 + p.selic_target_pct / 100.0) / (1 + i.ipca_accum_12m_pct / 100.0) - 1) * 100
        AS real_interest_rate_pct,
    ((1 + p.selic_target_pct / 100.0) / (1 + i.ipca_accum_12m_pct / 100.0) - 1) * 100 >= 0
        AS is_positive_real_rate
FROM policy_rate p
INNER JOIN ipca_compounded i USING (month)
-- Partial windows would understate 12-month inflation and overstate the real rate.
WHERE i.months_in_window = 12
