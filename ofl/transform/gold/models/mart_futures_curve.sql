-- mart_futures_curve — daily futures term structure across the FINANCIAL
-- (DI1, DOL, IND, WIN, WDO...) and AGRIBUSINESS (boi, milho, café, soja...)
-- contracts. Joins consolidated quotes to open interest and the instrument
-- registry (maturity, multiplier). Grain: symbol x date.
--
-- `asset`/`expiration_code` come from the open-interest fact (covers expired
-- contracts in history); `maturity`/`multiplier` come from the latest instrument
-- snapshot (best-effort — contracts that expired before the snapshot have null
-- maturity but keep their price/OI rows).
WITH q AS (
    SELECT symbol, date, segment, last_price, settlement_price, settlement_rate,
           ref_price, contracts, notional
    FROM fact_derivatives_quote
),
oi AS (
    SELECT symbol, date, asset, expiration_code, open_interest, open_interest_var
    FROM fact_open_interest
),
inst AS (
    SELECT symbol, asset AS asset_i, expiration_date, contract_multiplier, trading_ccy
    FROM dim_instrument
)
SELECT
    q.date,
    coalesce(oi.asset, inst.asset_i)        AS asset,
    q.symbol,
    oi.expiration_code,
    inst.expiration_date                    AS maturity,
    date_diff('day', q.date, inst.expiration_date) AS days_to_maturity,
    q.segment,
    q.last_price,
    q.settlement_price,
    q.settlement_rate,
    q.ref_price,
    q.contracts,
    q.notional,
    oi.open_interest,
    oi.open_interest_var,
    inst.contract_multiplier,
    inst.trading_ccy
FROM q
LEFT JOIN oi   ON oi.symbol = q.symbol AND oi.date = q.date
LEFT JOIN inst ON inst.symbol = q.symbol
ORDER BY asset, q.date, maturity;
