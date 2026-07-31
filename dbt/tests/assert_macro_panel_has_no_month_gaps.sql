-- The monthly macro panel must be a dense month grid.
--
-- Business rule, not a schema constraint: everything built on this panel uses
-- trailing-window functions (`ROWS BETWEEN 11 PRECEDING AND CURRENT ROW`), which
-- count *rows*, not months. A single missing month silently turns a 12-month
-- inflation window into a 13-month one and every downstream real rate is wrong by
-- an amount nothing else would flag. `unique`/`not_null` on `month` cannot catch
-- this — the gap is between the rows, not in them.

WITH bounds AS (

    SELECT
        count(*)                                          AS observed_months,
        date_diff('month', min(month), max(month)) + 1    AS expected_months,
        min(month)                                        AS first_month,
        max(month)                                        AS last_month
    FROM {{ ref('stg_gold__macro_monthly') }}

)

SELECT *
FROM bounds
WHERE observed_months <> expected_months
