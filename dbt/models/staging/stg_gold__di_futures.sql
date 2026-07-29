-- One-day-one-contract grain for the DI1 (one-day interbank deposit) futures strip
-- — the instrument the Brazilian nominal curve is actually read off.
--
-- `mart_futures_curve` carries every B3 asset, most of which are price-quoted and
-- have no `settlement_rate` at all. Narrowing to DI1 here keeps the rate-curve
-- models honest: everything downstream is a rate, in % p.a., on a single strip.
--
-- Known coverage limitation, measured on the export this lane was developed
-- against: 1,545 of 11,016 DI1 rows carry no `maturity`, and therefore no
-- `days_to_maturity`. They are exactly the *expired* near expirations (codes N25
-- through M26) — the instrument dimension the production mart joins to only
-- resolves currently-listed contracts, so a contract loses its expiration date
-- once it rolls off. Without a maturity a contract cannot be placed on a tenor
-- grid, so those rows are dropped here rather than guessed at. The visible cost
-- is a thinner short end: the 6m grid point in `mart_di_curve_points` exists on
-- 116 of 258 trade dates instead of all of them. Reconstructing the missing dates
-- from the B3 expiration code was tried and rejected — it reproduces the true
-- expiration month on 98.6% of the rows that can be checked, and a curve built on
-- a mostly-right maturity is worse than a curve that is honestly short.

SELECT
    date                                            AS trade_date,
    symbol                                          AS contract_symbol,
    expiration_code,
    maturity                                        AS expiration_date,
    days_to_maturity,
    days_to_maturity / 365.25                       AS tenor_years,
    settlement_rate                                 AS di_rate_pct,
    open_interest,
    contracts                                       AS contracts_traded,
    -- Contracts with no open interest carry stale settlement prices; flagged
    -- rather than dropped so the curve models can decide.
    coalesce(open_interest, 0) > 0                   AS has_open_interest,
    date::VARCHAR || '|' || symbol                  AS contract_key
FROM {{ source('gold', 'mart_futures_curve') }}
WHERE asset = 'DI1'
  AND settlement_rate IS NOT NULL
  -- Also excludes NULL: an unresolved expiration has no tenor. See header.
  AND days_to_maturity > 0
