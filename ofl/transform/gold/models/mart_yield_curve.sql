-- Yield curve: Tesouro Direto sell yields by bond and tenor over time.
SELECT
    date,
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
