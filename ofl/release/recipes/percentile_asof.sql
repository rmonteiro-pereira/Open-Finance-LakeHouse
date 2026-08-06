-- As-of historical percentile — the primitive that turns a level into an answer.
--
-- "IPCA+7,12%" informs nobody who does not already carry the series in their head.
-- "percentile 87 of the last decade" informs anyone. So the percentile belongs to the
-- substrate, published with its method, not re-derived by each consumer.
--
-- AS-OF: a row's rank is computed only against observations up to that row's OWN date.
-- Ranking against the full history would rewrite every past row on every release, the
-- checksums would churn, and the artefact would stop being pinnable.
--
-- What as-of does NOT buy is immutability of a row: the sources revise. BACEN restates
-- ibc_br, IBGE revises PNAD, the Focus median moves, and the landing is full-refresh.
-- `inputs_sha256` is here so a consumer can SEE that the window's inputs changed rather
-- than being promised they never do.
--
-- MID-RANK, fixed deliberately:
--
--     pct_rank = (n_below + 0.5 * n_ties) / n_obs
--
-- DuckDB offers `percent_rank()` = (rank-1)/(n-1) and `cume_dist()` = rank/n, and both
-- are called "percentile" in the wild. On a ten-year daily SELIC window with a long
-- plateau they differ by ~16 percentage points. A published number without a fixed
-- convention is a different number in every consumer, so `n_below` and `n_ties` are
-- published beside the result and anyone can recompute the other conventions.
--
-- Pure DuckDB SQL: no UDF, no extension. It runs in this repo's CI, in a consumer's CI
-- and inside the MCP server without anything being installed.
WITH w(window_label, years) AS (
    VALUES ('5y', 5), ('10y', 10), ('full', 200)
),
obs AS (
    SELECT series_id, date, value
    FROM fact_observation
    WHERE value IS NOT NULL
),
scored AS (
    SELECT
        o.series_id,
        o.date,
        o.value,
        w.window_label,
        -- Closed on the left, open at the start: (date - k years, date].
        (o.date - to_years(w.years))::DATE AS window_start,
        count(*)                                  AS n_obs,
        count(*) FILTER (WHERE p.value < o.value) AS n_below,
        count(*) FILTER (WHERE p.value = o.value) AS n_ties,
        min(p.date)                               AS first_obs_date,
        sha256(string_agg(p.date::VARCHAR || '=' || p.value::VARCHAR, ',' ORDER BY p.date))
            AS inputs_sha256
    FROM obs o
    CROSS JOIN w
    JOIN obs p
      ON p.series_id = o.series_id
     AND p.date <= o.date
     AND p.date > (o.date - to_years(w.years))::DATE
    GROUP BY 1, 2, 3, 4, 5
),
expected AS (
    -- How many observations a complete window WOULD hold, from the series' declared
    -- cadence. A relative floor is what lets a monthly series have a 5y window at all:
    -- an absolute cut of 120 observations silently disqualified every monthly series,
    -- whose 5y window holds 60 points by construction.
    SELECT
        s.series_id,
        w.window_label,
        CASE s.frequency
            WHEN 'daily'     THEN w.years * 252
            WHEN 'weekly'    THEN w.years * 52
            WHEN 'monthly'   THEN w.years * 12
            WHEN 'quarterly' THEN w.years * 4
            WHEN 'annual'    THEN w.years
        END AS n_expected
    FROM dim_series s
    CROSS JOIN w
    WHERE w.window_label <> 'full'
)
SELECT
    s.series_id,
    s.date,
    s.value,
    s.window_label,
    s.window_start,
    s.n_obs,
    s.n_below,
    s.n_ties,
    e.n_expected,
    (s.n_below + 0.5 * s.n_ties) / s.n_obs AS pct_rank,
    date_diff('day', s.first_obs_date, s.date) / 365.25 AS window_span_years,
    -- Three conditions, each closing a different hole: too few points to rank at all,
    -- a window with holes in it, and a series too young for the window it claims.
    (
        s.n_obs >= 24
        AND (e.n_expected IS NULL OR s.n_obs::DOUBLE / e.n_expected >= 0.9)
        AND date_diff('day', s.first_obs_date, s.date) / 365.25 >= 3
    ) AS percentile_allowed,
    'mid_rank' AS method,
    s.inputs_sha256
FROM scored s
LEFT JOIN expected e USING (series_id, window_label)
ORDER BY s.series_id, s.date, s.window_label;
