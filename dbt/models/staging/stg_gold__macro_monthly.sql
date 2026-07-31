-- Monthly macro panel, renamed to explicit units.
--
-- The panel is a *left-aligned* grid: every month between the first and last
-- observation is present, but individual series start at different dates
-- (`selic_target` only from the Copom-target series' first month, `debt_to_gdp_pct`
-- from the first fiscal statistics release). Nulls are therefore meaningful and
-- are deliberately kept here rather than filtered — downstream models decide.

SELECT
    month,
    selic_target    AS selic_target_pct,
    ipca_mom        AS ipca_mom_pct,
    usd_brl         AS usd_brl_rate,
    debt_to_gdp_pct AS debt_to_gdp_pct
FROM {{ source('gold', 'mart_macro_dashboard') }}
