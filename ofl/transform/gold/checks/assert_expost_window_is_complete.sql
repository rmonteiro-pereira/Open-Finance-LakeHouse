-- An ex-post window with a missing session is not a slightly worse number — it is a
-- different number wearing the same label. Compounding 249 daily rates where the
-- calendar had 252 understates the accumulated SELIC and overstates the real rate,
-- with nothing in the output to say so.
--
-- Returns violating rows; empty is a pass.
SELECT
    ref_month,
    window_start,
    window_end,
    n_business_days_expected,
    n_business_days_observed,
    n_business_days_expected - n_business_days_observed AS missing_sessions,
    'incomplete_business_day_window' AS failure_reason
FROM mart_real_interest_expost
WHERE n_business_days_observed <> n_business_days_expected
ORDER BY ref_month;
