-- Yield curve: Tesouro Direto sell yields by instrument and tenor over time.
--
-- `provider` and `data_class` are projected, and that is the point of this file's last
-- revision. `fact_treasury` receives Tesouro (real, ODbL) and ANBIMA (sandbox, values
-- FICTITIOUS by the provider's own description) through the same MERGE. Before this,
-- the mart carried no discriminator at all — the only thing separating a real price from
-- an invented one was a side effect of the `bond ILIKE` bucket below, which is a
-- presentation rule, not a provenance one.
SELECT
    date,
    instrument_id,
    provider,
    data_class,
    bond,
    maturity,
    date_diff('day', date, maturity) / 365.25 AS years_to_maturity,
    sell_rate AS yield,
    buy_rate,
    sell_price,
    CASE
        WHEN bond ILIKE '%IPCA%'      THEN 'ipca_plus'
        WHEN bond ILIKE '%Prefixado%' THEN 'prefixado'
        WHEN bond ILIKE '%Selic%'     THEN 'selic'
        ELSE 'other'
    END AS bond_type
FROM fact_treasury
WHERE sell_rate IS NOT NULL
  AND maturity > date
ORDER BY date, years_to_maturity;
