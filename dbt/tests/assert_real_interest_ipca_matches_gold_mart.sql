-- Cross-lane equivalence: the dbt re-expression of the 12-month IPCA compounding
-- must reproduce the production mart exactly.
--
-- This is the test that makes the dbt lane worth having as a check and not just a
-- keyword. `ofl/transform/gold/models/mart_real_interest.sql` compounds IPCA off
-- `fact_observation` inside the DuckDB gold runner; this project compounds it off
-- the exported macro panel through `ref()`. Two independent code paths, one
-- number — and the month *coverage* has to agree too, so a window-completeness
-- bug on either side surfaces as a missing month rather than passing silently.
--
-- The SELIC leg is intentionally not compared: the production mart averages
-- intra-month Copom observations while the export only retains the month-end
-- value. That divergence is documented in `mart_real_interest_dbt` and is a
-- property of the export, not a defect this test should hide.

WITH dbt_lane AS (

    SELECT month, ipca_accum_12m_pct
    FROM {{ ref('mart_real_interest_dbt') }}

),

production_lane AS (

    SELECT month, ipca_accum_12m
    FROM {{ source('gold', 'mart_real_interest') }}

)

SELECT
    coalesce(d.month, p.month)         AS month,
    d.ipca_accum_12m_pct               AS dbt_lane_value,
    p.ipca_accum_12m                   AS production_lane_value,
    CASE
        WHEN d.month IS NULL THEN 'missing_in_dbt_lane'
        WHEN p.month IS NULL THEN 'missing_in_production_lane'
        ELSE 'value_mismatch'
    END                                AS failure_reason
FROM dbt_lane d
FULL OUTER JOIN production_lane p USING (month)
WHERE d.month IS NULL
   OR p.month IS NULL
   OR abs(d.ipca_accum_12m_pct - p.ipca_accum_12m) > 1e-9
