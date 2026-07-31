-- The monthly macro panel must be a dense month grid.
--
-- Business rule, not a schema constraint: models built on monthly series use
-- trailing-window functions (`ROWS BETWEEN 11 PRECEDING AND CURRENT ROW`), which
-- count *rows*, not months. A single missing month silently turns a 12-month
-- inflation window into a 13-month one and every downstream real rate is wrong by
-- an amount nothing else would flag. Uniqueness/non-null checks on `month` cannot
-- catch this — the gap is between the rows, not in them. No unit test on
-- synthetic fixtures can catch a gap that only exists in production data either,
-- which is why this runs on every build.
WITH bounds AS (
    SELECT
        count(*)                                       AS observed_months,
        date_diff('month', min(month), max(month)) + 1 AS expected_months,
        min(month)                                     AS first_month,
        max(month)                                     AS last_month
    FROM mart_macro_dashboard
)
SELECT *
FROM bounds
WHERE observed_months <> expected_months
