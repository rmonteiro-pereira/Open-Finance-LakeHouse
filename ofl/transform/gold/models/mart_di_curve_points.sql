-- mart_di_curve_points — the DI1 strip resampled onto a fixed tenor grid, so
-- curves are comparable across trade dates.
--
-- Listed expirations roll: today's "roughly two years out" contract is 21 months
-- out next quarter, so reading a time series straight off contract symbols mixes
-- tenors. For each trade date and each grid tenor this mart brackets the target
-- with the nearest listed expiration on either side and interpolates linearly in
-- tenor between their settlement rates.
--
-- Deliberately no extrapolation: if the strip does not straddle a grid tenor on a
-- given date (typical at the long end, where listings thin out), no point is
-- emitted for it. A short strip should shorten the curve, not invent a rate for
-- it. `checks/assert_di_curve_points_are_bracketed.sql` enforces this after
-- every build.
--
-- Tenor is calendar-years (`days_to_maturity / 365.25`). DI1 rates are quoted on a
-- 252-business-day basis; converting to a business-day tenor would need a B3
-- holiday calendar, which the lakehouse does not carry. Calendar tenor is fine for
-- placing a contract on the grid — it is not used to compound anything.
--
-- Inputs are the silver facts directly (`fact_derivatives_quote` +
-- `dim_instrument`, plus `fact_open_interest` for the liquidity flag) — the same
-- tables `mart_futures_curve` reads — so this mart stays independent of the
-- other marts. Contracts whose expiration the instrument snapshot cannot resolve
-- (they expired before it) have no tenor and are dropped, not guessed at: the
-- visible cost is a thinner short end early in the history.
WITH strip AS (
    SELECT
        q.date                                               AS trade_date,
        q.symbol                                             AS contract_symbol,
        i.expiration_date,
        date_diff('day', q.date, i.expiration_date)          AS days_to_maturity,
        date_diff('day', q.date, i.expiration_date) / 365.25 AS tenor_years,
        q.settlement_rate                                    AS di_rate_pct,
        -- Contracts with no open interest carry stale settlement prices; flagged
        -- rather than dropped so consumers can decide.
        coalesce(oi.open_interest, 0) > 0                    AS has_open_interest
    FROM fact_derivatives_quote q
    JOIN dim_instrument i
      ON i.symbol = q.symbol AND i.asset = 'DI1'
    LEFT JOIN fact_open_interest oi
      ON oi.symbol = q.symbol AND oi.date = q.date
    -- DI1 is rate-quoted; most B3 assets are price-quoted with no settlement_rate.
    WHERE q.settlement_rate IS NOT NULL
      AND date_diff('day', q.date, i.expiration_date) > 0
),
grid (target_tenor_years, tenor_label) AS (
    VALUES (0.5, '6m'), (1.0, '1y'), (2.0, '2y'), (3.0, '3y'), (5.0, '5y'), (10.0, '10y')
),
candidates AS (
    SELECT strip.*, grid.target_tenor_years, grid.tenor_label
    FROM strip
    CROSS JOIN grid
),
-- Nearest listed expiration at or below the grid tenor.
lower_leg AS (
    SELECT
        trade_date,
        tenor_label,
        target_tenor_years,
        contract_symbol   AS lower_symbol,
        tenor_years       AS lower_tenor_years,
        di_rate_pct       AS lower_rate_pct,
        has_open_interest AS lower_has_open_interest
    FROM candidates
    WHERE tenor_years <= target_tenor_years
    QUALIFY row_number() OVER (
        PARTITION BY trade_date, tenor_label ORDER BY tenor_years DESC
    ) = 1
),
-- Nearest listed expiration at or above the grid tenor.
upper_leg AS (
    SELECT
        trade_date,
        tenor_label,
        contract_symbol   AS upper_symbol,
        tenor_years       AS upper_tenor_years,
        di_rate_pct       AS upper_rate_pct,
        has_open_interest AS upper_has_open_interest
    FROM candidates
    WHERE tenor_years >= target_tenor_years
    QUALIFY row_number() OVER (
        PARTITION BY trade_date, tenor_label ORDER BY tenor_years ASC
    ) = 1
)
SELECT
    l.trade_date,
    l.tenor_label,
    l.target_tenor_years,
    l.trade_date::VARCHAR || '|' || l.tenor_label AS curve_point_key,
    l.lower_symbol,
    u.upper_symbol,
    l.lower_tenor_years,
    u.upper_tenor_years,
    l.lower_rate_pct,
    u.upper_rate_pct,
    CASE
        WHEN u.upper_tenor_years = l.lower_tenor_years THEN l.lower_rate_pct
        ELSE l.lower_rate_pct
             + (u.upper_rate_pct - l.lower_rate_pct)
             * (l.target_tenor_years - l.lower_tenor_years)
             / (u.upper_tenor_years - l.lower_tenor_years)
    END AS di_rate_pct,
    -- How far the nearest listing sits from the grid point: the wider this is,
    -- the more of the quoted rate is interpolation rather than observation.
    greatest(
        l.target_tenor_years - l.lower_tenor_years,
        u.upper_tenor_years - l.target_tenor_years
    ) AS max_leg_gap_years,
    (l.lower_has_open_interest AND u.upper_has_open_interest) AS both_legs_have_open_interest
FROM lower_leg l
JOIN upper_leg u
  ON  l.trade_date = u.trade_date
  AND l.tenor_label = u.tenor_label
ORDER BY l.trade_date, l.target_tenor_years;
