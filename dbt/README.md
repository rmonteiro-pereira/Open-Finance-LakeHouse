# dbt lane over the gold marts

> **This is a parallel demonstration lane. It is not the production path.**
>
> The Open-Finance LakeHouse pipeline that actually runs — bronze → silver → gold,
> orchestrated by Airflow — does not use dbt and is not changed by anything in this
> directory. Gold is still built by `ofl/transform/gold/runner.py` executing the SQL
> in `ofl/transform/gold/models/`. This project sits *beside* that, reads the gold
> layer it produces, and re-expresses part of it in dbt. Nothing here was
> retrofitted into the pipeline, and no model in `ofl/` was rewritten to pretend it
> had always been dbt.

## Why it exists

The lakehouse already proves the modelling capability dbt is usually a proxy for:
layered models, dependency-ordered execution, tested outputs. It proves it with its
own DuckDB SQL runner. This lane demonstrates the same thing in the tool itself —
`source()` / `ref()` wiring, a materialisation strategy, generic and singular tests,
and generated docs — without disturbing a pipeline that already works.

It also earns its keep as a cross-check. `mart_real_interest_dbt` recomputes the
12-month IPCA compounding through a completely separate code path from the
production mart, and a singular test asserts the two agree month by month. That is
a genuine second opinion on a number the pipeline publishes, not a decorative test.

## How it relates to `ofl/transform/gold`

```
  silver Delta (MinIO)
        │
        │  ofl/transform/gold/runner.py   ← production path, unchanged
        │  + ofl/transform/gold/models/*.sql
        ▼
  gold Delta (MinIO)  ──export──►  ofl_gold.duckdb   ← this lane's only input
                                        │  READ_ONLY
                                        ▼
                                  dbt/  (this project)
```

The gold marts are this project's `source()`. They are read from a DuckDB database
attached `READ_ONLY` in `profiles.yml`, so no dbt command can write to the export
even by accident. dbt materialises its own models into a separate, throwaway DuckDB
file that this project owns.

The export is a snapshot, not a live connection. This lane deliberately does **not**
reach into MinIO — running it needs no credentials and no cluster.

### What could and could not be re-expressed

The export contains the gold marts only, not the silver facts they were built from.
So a model like `mart_macro_dashboard`, which pivots `fact_observation`, cannot be
recomputed here — its inputs are not in the file. What *can* be re-expressed is
logic whose inputs survive into gold, which is what `mart_real_interest_dbt` does.
The other two marts are new analytics built on top of the export rather than
re-derivations of existing ones.

## Models

| Model | Materialisation | Rows | What it does |
|---|---|---|---|
| `stg_gold__macro_monthly` | view | 384 | Monthly macro panel renamed to explicit units. Nulls kept — series start in different months. |
| `stg_gold__di_futures` | view | 9,471 | DI1 futures strip, one row per trade date × listed expiration, restricted to rate-quoted contracts with a resolvable maturity. |
| `mart_real_interest_dbt` | table | 327 | Ex-post real interest rate — the dbt counterpart of `ofl/transform/gold/models/mart_real_interest.sql`. |
| `mart_di_curve_points` | table | 1,392 | DI curve resampled onto a 6m/1y/2y/3y/5y/10y grid by linear interpolation between bracketing expirations. No extrapolation. |
| `mart_di_curve_slope` | table | 258 | One row per trade date: grid rates pivoted wide, 2s10s / 1s5s / 6m2y slopes, and a normal/flat/inverted label. |

The DAG is three levels deep and every edge is a real `ref()`:
`source(gold) → stg_gold__di_futures → mart_di_curve_points → mart_di_curve_slope`.

## Tests

53 nodes build clean: 5 models and **48 data tests**, all passing.

**Generic** — `not_null`, `unique`, `accepted_values` (tenor labels, tenor years,
curve shape, boolean flags) and `relationships` (curve points and slopes must trace
back to contracts that actually exist in the strip).

**Singular** — three, each encoding a rule that no generic test can express:

- **`assert_real_interest_ipca_matches_gold_mart.sql`** — cross-lane equivalence.
  The dbt lane's 12-month IPCA compounding must reproduce the production mart's
  `ipca_accum_12m` to within 1e-9 on every month, *and* the two month sets must be
  identical, so a window-completeness bug on either side shows up as a missing month
  rather than passing quietly. Both lanes currently produce the same 327 months with
  zero difference.
- **`assert_macro_panel_has_no_month_gaps.sql`** — the macro panel must be a dense
  month grid. Everything built on it uses `ROWS BETWEEN 11 PRECEDING AND CURRENT ROW`,
  which counts *rows*, not months: one missing month silently turns a 12-month
  inflation window into a 13-month one and corrupts every downstream real rate.
  `unique` and `not_null` on `month` cannot catch this — the gap is between the rows.
- **`assert_di_curve_points_are_bracketed.sql`** — every grid point must be an
  interpolation, never an extrapolation: the two source expirations must straddle the
  grid tenor, and the interpolated rate must land inside the two settlement rates it
  came from. The second check is what catches a flipped interpolation weight, which
  still returns a plausible-looking number that `not_null` would happily accept.

## Honest limitations

Two are worth stating plainly, because both are visible in the output.

**1. The SELIC leg of `mart_real_interest_dbt` is not identical to production.**
The production mart reads `fact_observation` and averages every SELIC-target
observation inside the month. This lane's only SELIC input is the exported macro
panel, which already collapsed each month to its *last* observation. In months
containing a Copom decision the two disagree — by up to **4.85 p.p.** across the 327
shared months. The IPCA leg, which is what the compounding logic actually exercises,
is identical to floating-point precision, and that is what the singular test asserts.
The real-rate column is therefore a month-end variant, not a reproduction, and the
model header says so.

**2. The DI short end is thin.** 1,545 of 11,016 DI1 rows in the export carry no
`maturity` — they are exactly the expired near expirations (codes `N25` through
`M26`), because the instrument dimension the production mart joins against only
resolves currently-listed contracts. A contract with no expiration date cannot be
placed on a tenor grid, so those rows are dropped. The cost is visible: the 6m grid
point exists on 116 of 258 trade dates, and the 1y point on 244. Reconstructing the
missing maturities from the B3 expiration code was tried and rejected — it recovers
the true expiration month on 98.6% of the rows that can be checked, and a curve built
on a mostly-right maturity is worse than a curve that is honestly short.

## Running it

The lane needs its own environment. dbt is declared as an optional extra in the root
`pyproject.toml` (`dbt = ["dbt-core>=1.11,<2", "dbt-duckdb>=1.10,<2"]`) and is not a
runtime dependency of `ofl` — nothing under `ofl/` imports it.

```bash
# from the repo root — isolated venv, so the project's own lockfile is untouched
uv venv dbt/.venv --python 3.11
uv pip install --python dbt/.venv/Scripts/python.exe "dbt-core>=1.11,<2" "dbt-duckdb>=1.10,<2"
```

Point the lane at a gold export and build:

```bash
cd dbt
export OFL_GOLD_DUCKDB=/path/to/ofl_gold.duckdb    # PowerShell: $env:OFL_GOLD_DUCKDB="..."

.venv/Scripts/dbt build --profiles-dir .           # run + test in dependency order
.venv/Scripts/dbt docs generate --profiles-dir .   # manifest + catalog
.venv/Scripts/dbt docs serve --profiles-dir .      # browse the DAG and column docs
```

`OFL_DBT_TARGET_DB` overrides where dbt materialises (default `ofl_dbt_dev.duckdb`
inside `dbt/`). Both that file and `target/` are gitignored; deleting them is always
safe.

Validated against `dbt-core 1.12.0` / `dbt-duckdb 1.10.1` / DuckDB 1.5.5, on a gold
export carrying 8 marts and 1,621,818 rows in `mart_futures_curve`:

```
Done. PASS=53 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=53
```

### Committed docs artifacts

`docs/manifest.json` (757 KB) and `docs/catalog.json` (6.7 KB) are a committed
snapshot of the last `dbt docs generate`, so the DAG and column-level documentation
can be inspected without installing dbt or having a gold export at hand. They contain
no absolute paths and no data. The rendered `index.html` is not committed — run
`dbt docs serve` to view it.
