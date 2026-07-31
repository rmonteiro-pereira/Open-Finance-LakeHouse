-- Independent recomputation of the 12-month IPCA compounding, reconciled against
-- the freshly built mart to 1e-9 p.p. on every month.
--
-- `mart_real_interest` compounds IPCA with `exp(sum(ln(1 + x)))` over a
-- *row-counted* window (`ROWS BETWEEN 11 PRECEDING AND CURRENT ROW`) — a window
-- that silently spans 13+ calendar months when a month is missing upstream. This
-- check recomputes each month's accumulation through a different code path:
-- `product()` over an explicit calendar-month self-join that must find exactly
-- the 12 calendar months ending at the target month. Different windowing
-- (calendar-interval, not row-count), different aggregation (product, not
-- exp/sum/ln) — so a shared bug has nowhere to hide, and a row-window corruption
-- surfaces as `mart_month_without_full_calendar_window` instead of passing
-- silently.
--
-- Coverage must agree too (FULL OUTER JOIN): a month present on one side only is
-- a violation with its own reason, so a window-completeness bug on either side
-- shows up as a missing month rather than a near-miss value.
WITH ipca AS (
    SELECT date_trunc('month', date)::DATE AS month, value AS ipca_mom
    FROM fact_observation
    WHERE series_id = 'ipca'
),
-- The mart only emits months that have a SELIC observation (it inner-joins the
-- policy rate); mirror that so coverage is comparable.
selic_months AS (
    SELECT DISTINCT date_trunc('month', date)::DATE AS month
    FROM fact_observation
    WHERE series_id = 'selic_meta'
),
recomputed AS (
    SELECT
        cur.month,
        (product(1 + w.ipca_mom / 100.0) - 1) * 100 AS ipca_accum_12m_recomputed
    FROM ipca cur
    JOIN ipca w
      ON  w.month >= cur.month - INTERVAL 11 MONTH
      AND w.month <= cur.month
    GROUP BY cur.month
    -- Exactly the 12 calendar months ending at cur.month, all present.
    HAVING count(*) = 12
),
expected AS (
    SELECT r.month, r.ipca_accum_12m_recomputed
    FROM recomputed r
    JOIN selic_months s USING (month)
),
mart AS (
    SELECT month, ipca_accum_12m
    FROM mart_real_interest
)
SELECT
    coalesce(e.month, m.month)  AS month,
    e.ipca_accum_12m_recomputed,
    m.ipca_accum_12m,
    CASE
        WHEN e.month IS NULL THEN 'mart_month_without_full_calendar_window'
        WHEN m.month IS NULL THEN 'missing_in_mart'
        ELSE 'value_mismatch'
    END AS failure_reason
FROM expected e
FULL OUTER JOIN mart m USING (month)
WHERE e.month IS NULL
   OR m.month IS NULL
   OR abs(e.ipca_accum_12m_recomputed - m.ipca_accum_12m) > 1e-9
