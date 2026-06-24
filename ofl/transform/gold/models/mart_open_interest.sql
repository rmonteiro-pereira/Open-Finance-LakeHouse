-- mart_open_interest — daily open interest aggregated by underlying asset
-- (DI1, DOL, IND, WIN, boi, milho...). Grain: asset x date. Total OI across all
-- maturities, day-over-day change, and contract breadth (how many maturities are
-- live). The crowd-positioning signal the lakehouse previously lacked.
SELECT
    date,
    asset,
    any_value(segment)              AS segment,
    sum(open_interest)              AS total_open_interest,
    sum(open_interest_var)          AS total_open_interest_var,
    count(DISTINCT symbol)          AS n_contracts
FROM fact_open_interest
WHERE asset IS NOT NULL
GROUP BY date, asset
ORDER BY asset, date;
