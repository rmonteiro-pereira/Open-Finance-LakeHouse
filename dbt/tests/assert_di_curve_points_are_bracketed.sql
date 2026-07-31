-- Every grid point must be an interpolation, never an extrapolation.
--
-- The business rule the curve rests on: a quoted tenor is only meaningful if the
-- listed strip actually straddles it. Two things have to hold on every row —
--   1. the bracketing expirations sit on opposite sides of the grid tenor, and
--   2. the interpolated rate lands inside the two settlement rates it came from.
-- (2) is the one that catches a swapped numerator/denominator or a flipped sign
-- in the interpolation weight: such a bug still produces a number, still passes
-- `not_null`, and is only wrong by an amount that looks like a market move.
--
-- Tolerance is 1e-9 on the tenor comparisons and 1e-9 p.p. on the rate — both are
-- floating-point slack, not modelling slack.

WITH points AS (

    SELECT
        curve_point_key,
        trade_date,
        tenor_label,
        target_tenor_years,
        lower_tenor_years,
        upper_tenor_years,
        lower_rate_pct,
        upper_rate_pct,
        di_rate_pct
    FROM {{ ref('mart_di_curve_points') }}

)

SELECT
    *,
    CASE
        WHEN lower_tenor_years > target_tenor_years + 1e-9 THEN 'lower_leg_past_target'
        WHEN upper_tenor_years < target_tenor_years - 1e-9 THEN 'upper_leg_before_target'
        ELSE 'interpolated_rate_outside_legs'
    END AS failure_reason
FROM points
WHERE lower_tenor_years > target_tenor_years + 1e-9
   OR upper_tenor_years < target_tenor_years - 1e-9
   OR di_rate_pct < least(lower_rate_pct, upper_rate_pct) - 1e-9
   OR di_rate_pct > greatest(lower_rate_pct, upper_rate_pct) + 1e-9
