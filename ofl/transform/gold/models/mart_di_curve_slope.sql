-- mart_di_curve_slope — daily shape of the DI curve: one row per trade date,
-- grid rates pivoted wide, the standard slopes, and a normal/flat/inverted label.
--
-- Built entirely from `mart_di_curve_points` — the one mart-on-mart edge in the
-- gold layer, so the tenor grid and its interpolation are defined in exactly one
-- place. The runner registers each computed mart back into the connection in
-- `MODELS` order, which is why `mart_di_curve_points` is queryable here by name.
--
-- The classification threshold is 10 bp. Anything inside ±10 bp on the 2s10s is
-- called flat rather than signed: at that magnitude the sign is dominated by the
-- interpolation between listed expirations, not by the market's view.
WITH points AS (
    SELECT trade_date, tenor_label, di_rate_pct
    FROM mart_di_curve_points
),
pivoted AS (
    SELECT
        trade_date,
        max(di_rate_pct) FILTER (WHERE tenor_label = '6m')  AS rate_6m_pct,
        max(di_rate_pct) FILTER (WHERE tenor_label = '1y')  AS rate_1y_pct,
        max(di_rate_pct) FILTER (WHERE tenor_label = '2y')  AS rate_2y_pct,
        max(di_rate_pct) FILTER (WHERE tenor_label = '3y')  AS rate_3y_pct,
        max(di_rate_pct) FILTER (WHERE tenor_label = '5y')  AS rate_5y_pct,
        max(di_rate_pct) FILTER (WHERE tenor_label = '10y') AS rate_10y_pct,
        count(*) AS n_curve_points
    FROM points
    GROUP BY trade_date
)
SELECT
    trade_date,
    rate_6m_pct,
    rate_1y_pct,
    rate_2y_pct,
    rate_3y_pct,
    rate_5y_pct,
    rate_10y_pct,
    n_curve_points,
    rate_10y_pct - rate_2y_pct AS slope_2s10s_pp,
    rate_5y_pct  - rate_1y_pct AS slope_1s5s_pp,
    rate_2y_pct  - rate_6m_pct AS slope_6m2y_pp,
    CASE
        WHEN rate_10y_pct IS NULL OR rate_2y_pct IS NULL THEN NULL
        WHEN rate_10y_pct - rate_2y_pct >  0.10          THEN 'normal'
        WHEN rate_10y_pct - rate_2y_pct < -0.10          THEN 'inverted'
        ELSE 'flat'
    END AS curve_shape
FROM pivoted
ORDER BY trade_date;
