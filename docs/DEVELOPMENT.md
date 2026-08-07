# Development — the data-product work

For whoever picks this branch up cold, human or agent. Not a pitch: the README is the pitch.
This file says what is actually here, what is only designed, what will bite you, and which
command settles each question.

**Everything below was measured on `feat/produto-de-dados` @ `9f1aeaa` on 2026-08-07.** Where a
claim could not be executed — anything behind Spark, anything behind a credential — it is marked
**unverified** rather than asserted. Re-measure before trusting: the point of the whole exercise
is that a number carries its provenance.

---

## 1. What this is, and what the thesis is

The Open-Finance LakeHouse is a medallion pipeline over Brazilian public macro and financial data:
Polars extracts to bronze Delta, Spark conforms to a silver star schema, DuckDB serves gold marts.
That part predates this branch and is described in the README.

**What this branch changes is what counts as the product.**

The repository used to be measured in units of *supply* — 51 registered series, 10 handlers,
5 facts, 10+ marts. Every one of those numbers is true and none of them moves when somebody uses
the thing. Measured from the demand side the position was: the only human surface was a Next.js
dashboard that was not in git, deployed behind Cloudflare Access, serving a **synthetic** snapshot
by default; every programmatic path needed MinIO credentials from a home Kubernetes cluster.

The thesis of this work, recorded as `docs/adr/0002-the-published-release-is-the-product.md`:

> **The interface is a published immutable release, not a screen.**

A consumer — a person, a script, an LLM agent — should be able to fetch a versioned artefact with
a `manifest.json`, checksums, schema contracts and stated provenance, and get a real Brazilian
number without a credential, a cluster, or this repository. The dashboard is demoted from
"the product" to *the first consumer of the product*, which is what makes it evidence that the
artefact is usable rather than decoration.

Everything follows from that. The gates exist because an immutable artefact you cannot retract is
the one place a wrong number is most expensive. The governing rule, from the RFC:

> A percentile over the wrong grain is a false number with impeccable provenance — the worst
> artefact this project can produce.

Design of record:

| Document | What it is |
|---|---|
| `E:/Projetos/Portifolio/_specs/rfc-ofl-produto-de-dados.md` | RFC v4, approved for implementation. Survived two adversarial rounds. |
| `E:/Projetos/Portifolio/_specs/DECISOES-OFL.md` | D1–D17, each with a **reversal criterion**. See §8. |
| `docs/adr/0002-the-published-release-is-the-product.md` | The one-page version, in-repo. |
| `docs/DEPLOY.md` | The human steps nobody has executed yet. |

---

## 2. Current state — measured, not copied from the RFC

```
worktree   E:/Projetos/Portifolio/_wt-OFL-product
branch     feat/produto-de-dados
HEAD       9f1aeaa
base       origin/main @ b21dfc0   (16 ahead, 0 behind)
tree       clean
tests      313 passed in 54.75s
```

Reproduce:

```bash
git -C E:/Projetos/Portifolio/_wt-OFL-product log --oneline origin/main..HEAD
git -C E:/Projetos/Portifolio/_wt-OFL-product status --porcelain
```

> **Do not work in `E:/Projetos/Portifolio/Open-Finance-LakeHouse`.** That checkout sits on a
> divergent `docs/portfolio-polish` with a dirty tree. It is not the base of this work and it is
> not evidence about what is published. `git show origin/main:<path>` is how you ask what is
> published. (D6.)

Nothing on this branch has been pushed. That is a decision, not an oversight — D7: push to a
**public** repository is an act of publication, of a different category from writing code, and
the house process puts CodeRabbit before a push. Reversible on one word from Rodrigo.

### 2.1 What is implemented vs. only designed

Verified by reading the files and running the suite; the RFC's own checkmarks were **not** trusted.

| RFC phase | Verdict | Evidence |
|---|---|---|
| **F0** — no synthetic snapshot in git | **done** | `dashboard/snapshot/gen_synthetic.py` absent; `git ls-files dashboard/public/data` → 0; ignore rule at `dashboard/.gitignore:49` |
| **C4** — Brazilian business-day calendar | **done** | `ofl/calendar.py` (`easter:36`, `brazilian_holidays:57`, `build_calendar:87`); `tests/test_calendar.py`; commit `65d38cb` |
| **C1** — grain of `fact_treasury` | **mostly** — see gap below | `ofl/transform/keys.py`; `ofl/transform/spark/silver.py:156,167,186`; `tests/test_keys.py` (16 tests); frozen vectors `tests/fixtures/golden/instrument_id_vectors.csv` |
| **C2** — provenance columns | **mostly** — see gap below | `ofl/ingestion/landing.py:37` (`data_class` keyword-only, no default); all 10 handlers pass it; `mart_yield_curve.sql` projects `provider`/`data_class` |
| **C3** — real interest split | **done** | `mart_real_interest_exante.sql`, `mart_real_interest_expost.sql`, `checks/assert_expost_window_is_complete.sql`; old mart kept as deprecated at `runner.py:38` |
| **C5** — unit trio | **done** | `sources/registry.yml` (trio per series); `ofl/registry.py:45-50`; golden `tests/fixtures/registry_units.expected.csv`; `tests/test_units.py` reads **raw YAML** |
| **F1** acceptance canaries | **done except (v)** | `tests/test_keys.py:120,133,146,154,174,225` |
| **F2** — the release artefact | **partial** — see §2.2 | `ofl/release/{build,verify,gates,contracts,publish}.py`; `tests/test_release.py` (22 tests) |
| **F3** — publish + watchdog | **function done, deployment not** | `ofl/release/publish.py:42` `health()`; `tests/test_publish.py`; `publish/ofl-public-data/.github/workflows/watchdog.yml`; `docs/DEPLOY.md` |
| **F4** — the site | **done** | 6 routes under `dashboard/src/app`; `dashboard/next.config.ts:7` `output: "export"`; `dashboard/snapshot/from_release.py`; job `dashboard` at `.github/workflows/tests.yml:55` |
| **F5** — agent surface | **done, with a deviation** | `ofl/mcp/tools.py`, `ofl/mcp/server.py`, `ofl/mcp/evals.py`; `evals/threshold.json` = `1.0`; 15 cases in `tests/evals/gold.yaml` |

### 2.2 Designed but **not built** — do not assume these exist

Grepped for by name across the whole tree; zero hits each.

| RFC id | Missing thing | Consequence |
|---|---|---|
| **P2b / C1(v)** | `dim_instrument_map` | The "a new raw label makes the build **fail closed**" acceptance is unmet. Today a spelling change at the source re-keys the whole history silently, and `revised_rows_vs_previous` reports `0` because it matches by key and nothing matches. This is the single largest hole on the branch. |
| **P3** | `mart_tesouro_curve_grid`, `mart_tesouro_breakeven` | No treasury curve grid, no breakeven. `mart_yield_curve.sql` is the only treasury mart. |
| **P6** | `catalog.json`, `dim_series_semantic` | No authored semantics layer. The MCP `describe_series` reads the registry directly. |
| **P7** | `series_caveats` | `has_declared_break_in_window` has no source table; structural breaks are unannounced. |
| **P1** | `latest.json`, `recipes/*`, `golden/*.csv`, `ofl_public.duckdb` **inside the built release** | `latest_pointer()` exists (`publish.py:99`) but `build_release` never writes it. A built release directory contains only `manifest.json`, `checksums.sha256`, `contracts/`, `parquet/` — verified by running it (§4.3). |
| **P9/CLI** | `ofl release publish` | `ofl/release/publish.py:116` `publish()` is written and tested, but `ofl/cli.py:272-293` registers only `build` and `verify`. No CLI path reaches the sink. |
| **P12** | `health.json` | Blocked on the `ofl-public-data` repository, which is a **human step** (D16). Until it exists the product has one channel and one author, and the author is the producer — exactly who cannot attest to its own death. |

---

## 3. Architecture map

### 3.1 The medallion path

```
sources/registry.yml ──drives everything──┐
                                          │
Polars   ofl/ingestion/*.py  ──►  bronze Delta (MinIO)      per-series tables
          land_bronze() stamps provider, data_class, ingested_at, load_id
          tesouro.py also stamps instrument_id  ← computed ONCE, here
                                          │
Spark    ofl/transform/spark/silver.py ──►  silver star schema
          conform_observations / conform_treasury / conform_security_prices
          + ofl/transform/spark/dimensions.py  (dim_series, dim_date, dim_instrument)
                                          │
DuckDB   ofl/transform/gold/runner.py  ──►  gold marts
          12 models in models/, 4 post-build checks in checks/
                                          │
Release  ofl/release/build.py  ──►  an immutable directory / GitHub Release
          reader (--from) + sink (--to), gates in between
                                          │
                        ┌─────────────────┴─────────────────┐
              dashboard/ (Next.js static export)     ofl/mcp/ (agent tools)
              reads only a published release          reads only a DuckDB conn
```

### 3.2 Where each layer lives

| Layer | Path | Notes |
|---|---|---|
| Source registry | `sources/registry.yml` | 51 series. Carries the unit trio per series. |
| Licence registry | `sources/providers.yml` | Exactly **10 handler keys** — `bacen_sgs`, `bacen_focus`, `tesouro_direto`, `ipea`, `ibge`, `anbima`, `b3`, `b3_cotahist`, `b3_arquivos`, `yahoo`. This closed domain is what the `license` gate checks membership against. |
| Registry loaders | `ofl/registry.py`, `ofl/providers.py` | `is_redistributable:77`, `assert_publishable:98` |
| Bronze | `ofl/ingestion/` | 10 extractors + `landing.py:37` `land_bronze` |
| Keys (pure) | `ofl/transform/keys.py` | `instrument_id:74`, `assert_grain_is_not_coarser:161`, `dedup_latest:183`. **Importable without Spark** — that is the whole design. |
| Calendar (pure) | `ofl/calendar.py` | Same reason. `dimensions.py:101` calls into it. |
| Silver | `ofl/transform/spark/silver.py`, `dimensions.py` | Spark. Not exercisable in CI (§4.4). |
| Gold | `ofl/transform/gold/` | `runner.py:31` `MODELS` (12), `runner.py:58` `CHECKS` (4) |
| Release | `ofl/release/` | `build.py`, `verify.py`, `gates.py`, `contracts.py`, `publish.py`, `recipes/percentile_asof.sql` |
| Agent surface | `ofl/mcp/` | `tools.py` (pure, zero SDK import), `server.py` (shell), `evals.py` |
| Reader surface | `dashboard/` | `src/app/*` routes, `src/lib/release.ts` (only loader), `snapshot/from_release.py` |

### 3.3 The CLI (`ofl/cli.py`)

`[project.scripts]` declares exactly one entrypoint: `ofl = "ofl.cli:main"`.

| Command | Line | Needs |
|---|---|---|
| `ofl registry` | 211 | nothing |
| `ofl ingest [--series\|--domain]` | 21 | network + MinIO |
| `ofl silver` | 38 | Spark + MinIO |
| `ofl gold [--dry-run]` | 51 | DuckDB + MinIO |
| `ofl release build --from --to --release-id --release-class` | 65 | **nothing** — pure local |
| `ofl release verify <dir> [--expect-class]` | 87 | **nothing** |
| `ofl evals [--corpus]` | 103 | **nothing** |
| `ofl stream-*` (5 subcommands) | 118-207 | Spark streaming lane |

The MCP server is *not* a CLI subcommand: run it as `python -m ofl.mcp.server --release ./out`
(`ofl/mcp/server.py:82`), with the `mcp` extra installed.

`PUBLISHABLE_TABLES` at `ofl/cli.py:62` is an **allowlist of tables** — `fact_observation` and
`fact_tesouro_direto`, nothing else. `read_source` (`build.py:69`) **aborts** on any other table
rather than skipping it, because the repo's only `.duckdb` producer copies every mart out of MinIO
with no filter at all, and "ignore what I do not recognise" is the exact shape a leak would take.

---

## 4. Running things

### 4.1 Install

```bash
uv sync --extra dev          # <- the dev extra is NOT optional in practice
```

`uv sync` alone installs the core (Polars + DuckDB + deltalake) and **does not install pytest**.
This is a real trap: a bare `uv sync` followed by `pytest` fails with `program not found`, which
reads like a broken repo and is not. Other extras: `spark`, `streaming`, `airflow`, `yahoo`,
`lineage`, `mcp` — declared in `pyproject.toml:29-47`.

### 4.2 Tests

```bash
uv run pytest -q             # 313 passed in 54.75s, measured 2026-08-07
```

No cluster, no MinIO, no Spark session, no API keys, no network. That is deliberate and it is what
makes the badge mean anything (`.github/workflows/tests.yml`, header comment).

### 4.3 The release path, end to end, offline

This is the fastest way to convince yourself the product exists. All four commands were run and
their exit codes are the ones below.

```bash
ofl release build --from tests/fixtures/release --to out \
    --release-id 1970-01-01.1 --release-class fixture     # exit 0
ofl release verify out --expect-class fixture             # exit 0
ofl release verify out                                    # exit 3  ← gate `class`
ofl release verify /does/not/exist                        # exit 2  ← usage, not a verdict
ofl release build ... --release-id 0000-00-00.1           # exit 3  ← gate `release_id_format`
```

**Exit codes are load-bearing.** `2` = the question was malformed. `3` = the answer was no. A
negative test that only asserts "non-zero" is satisfied by a typo in a path, which is how a gate
gets to pass for the wrong reason forever. `out/verify_report.json` carries
`{failed_gate, table, detail}` on disk so the reason is inspectable after the fact
(`ofl/release/verify.py:169`).

`1970-01-01.1` is the **fixture sentinel**: a real, parseable date that is obviously not
production. `0000-00-00.1` is not a date at all — `date.fromisoformat` raises on it — which is why
it is the negative canary rather than the sentinel.

What a built release actually contains, verified by `find`:

```
manifest.json
checksums.sha256
contracts/fact_observation.contract.json
contracts/fact_tesouro_direto.contract.json
parquet/fact_observation.parquet
parquet/fact_tesouro_direto.parquet
```

No `latest.json`, no `recipes/`, no `golden/`, no `ofl_public.duckdb` — see §2.2.

### 4.4 Spark does not run in CI, and what that costs you

`.github/workflows/tests.yml:44-46` installs `-e ".[dev]"`. **The `spark` extra is not installed.**
There is no Spark session anywhere in CI, and there is no local Spark run in this worktree either.

Concretely, the following are **unverified by execution** and can only be reviewed by reading:

- `conform_observations`, `conform_treasury`, `conform_security_prices` — `ofl/transform/spark/silver.py`
- `build_dim_series`, `build_dim_instrument`, `build_dim_date` — `ofl/transform/spark/dimensions.py`
- the whole streaming lane — `ofl/streaming/`

The mitigation is structural, not aspirational: **anything consequential is extracted into a pure
function that CI can reach**, and Spark consumes the same object.

- The treasury grain key is a module-level tuple, `keys.TREASURY_KEY`, not a string literal inside
  a PySpark call. `silver.py:186` builds its `MERGE` predicate with `merge_condition(TREASURY_KEY)`,
  and `tests/test_keys.py:249` asserts silver consumes that same tuple.
- The identifier has one implementation, `keys.instrument_id`, evaluated once in bronze.
- The calendar is `ofl/calendar.py`, in Polars; `dimensions.py:111` imports it.

**When you change Spark code, say so.** A green suite does not cover it. Write the pure twin, test
the twin, and have Spark call the twin.

### 4.5 The dashboard

```bash
ofl release build --from tests/fixtures/release --to out --release-id 1970-01-01.1 --release-class fixture
cd dashboard
python snapshot/from_release.py --release ../out    # writes public/data/*.json
pnpm install --frozen-lockfile && pnpm build        # static export to dashboard/out
```

`from_release.py` **refuses** a release whose manifest has any gate not `pass`, and missing data is
a build error, not a fallback. `dashboard/public/data/*.json` is gitignored
(`dashboard/.gitignore:49`) — the site ships with no data in git, by D9. Note that
`dashboard/package.json` has no script wiring `from_release.py` into `pnpm build`; the CI job does
it as a separate step and so must you.

### 4.6 What needs credentials

| Needs nothing | Needs MinIO / cluster | Needs a token |
|---|---|---|
| `ofl registry`, `ofl release build/verify`, `ofl evals`, `pytest`, the dashboard build, `python -m ofl.mcp.server` | `ofl ingest`, `ofl silver`, `ofl gold`, `ofl stream-*` | live `gh://` publish (does not exist yet — `publish.py:155` raises and points at `docs/DEPLOY.md`) |

---

## 5. The gates

Every gate returns a **named** result (`GateResult`, `ofl/release/gates.py:35`). Negative tests
assert the *name*, never merely a non-zero exit.

| Gate | Blocks | Implemented at | Status |
|---|---|---|---|
| `release_id_format` | yes | `gates.py:51` | ✅ live in `build_release` (`build.py:170`) |
| `contracts` | yes | `gates.py:71` + `contracts.py:73` `compare` | ✅ build + verify. Exact equality of `(name, arrow_type, arrow_nullable)`; an **extra** column fails. |
| `grain` (i) pre-dedup | yes | `keys.py:161` `assert_grain_is_not_coarser` (Polars) and `silver.py:156` (Spark) | ✅ both lanes. This is the half that can fail — see §6.1. |
| `grain` (ii) published frame | yes | `gates.py:80` | ✅ build + verify |
| `required` | yes | `gates.py:113` | ✅ build + verify. This is where "provider is mandatory" is really enforced. |
| `class` | yes | `gates.py:135`, structural half at `verify.py:149` | ✅ build + verify |
| `license` | yes | `gates.py:174`, structural half at `verify.py:146` | ✅ build + verify. Domain membership is checked **before** redistributability. |
| `key_drift` | yes | `gates.py:199` | ⚠️ **implemented but inert** — see below |
| `golden` | yes | *(no gate function)* | ⚠️ **not a gate.** Lives only as `tests/test_percentile.py:163`. |
| freshness | **no**, on purpose | inputs published at `build.py:296` `_series_block` | ✅ correct as designed |

### ⚠️ `key_drift` never bites in practice

`gates.py:199` is correct, and `build.py:130` `_key_drift` computes the diff — but it needs a
`previous` argument, and `ofl/cli.py:65` `_release_build` never passes one. `build_release`'s
`previous` defaults to `None`, so `rows_previous == 0`, so `gates.py:210` returns pass
unconditionally. `revised_rows_vs_previous` and `first_changed_date` are hardcoded `None` at
`build.py:222-223`. **The mass-re-keying alarm is not armed.** Wiring it needs the previous
release's key set, which needs a previous release — which is exactly what does not exist yet.

### ⚠️ `golden` is a pytest assertion, not a release gate

The RFC's gate table lists `golden` as blocking. There is no `gate_golden` in `gates.py` and no
`golden/` output in a built release. The mutation canary — swap mid-rank for `cume_dist` and watch
it fail — is real and passes, but it lives in the test suite, so it gates *the commit*, not *the
release*. Given the release is currently only ever built inside CI behind that same suite, the
practical difference is small today and large the day a release is built anywhere else.

### Freshness is deliberately not a gate

Blocking publication on a source outage pins every consumer to a stale release **with no signal**,
which is strictly worse than publishing the old observation date and letting the consumer see it.
So the manifest publishes `last_observation_date` and `freshness_budget_hours` **per series** and
never a verdict. A manifest from three days ago still answers "red" correctly without being
republished; one that declares itself "ok" lies on precisely the day the producer dies.

Freshness lives at the grain of the **series**, never the table: `fact_observation` unions daily
through quarterly series, so one `last_observation_date` for the whole table reads green whenever
any daily series is current, with a whole domain three months stale.

---

## 6. Known traps

These are landmines. Each one was stepped on at least once already, some twice — by a design round
correcting the previous design round.

### 6.1 The tautological `grain` gate

The original defect: `fact_treasury` merged on `(bond, date)` while `maturity` sat in the row and
outside the key, so two rows differing only by maturity collided and one was dropped — no error,
no log, no metric.

The first fix added a `grain` gate: `COUNT(*) == COUNT(DISTINCT pk)`. It ran **after** the dedup,
on the materialised table.

> A key that is too coarse satisfies `COUNT(*) = COUNT(DISTINCT pk)` **by construction, precisely
> because the dedup deleted the rows that would have shown the collision.** The gate created to
> close the gap left by defect A was logically incapable of failing on defect A.

The fix is part (i): run it on the **union of bronze, before any deduplication**, and **fail**
instead of deduplicating. `keys.py:52` `GrainError` says this in its own docstring;
`tests/test_keys.py:154` `test_grain_gate_runs_before_dedup_not_after` is the proof.

**The general form of this trap:** any predicate placed downstream of the thing that erases its
own counterexample. Before you believe a gate, build the input that should fail it and watch it
fail. `gate_grain` (`gates.py:80`) is still there and still useful — it catches nulls in key
components and post-transform duplication — but it is the *second* half, not the gate.

### 6.2 `instrument_id` derives from the **raw label**, never a bucket

```python
instrument_id = sha1(f"{provider}|{bond_label}|{maturity.isoformat()}")   # maturity NOT NULL
```

`bond_label` is `Tipo Titulo` exactly as the source publishes it. It is emphatically **not**
`bond_type`, the three-way `CASE ... ILIKE` bucket in `mart_yield_curve.sql`. That bucket collapses
"Tesouro IPCA+ 2035" (NTN-B Principal, zero-coupon) with "Tesouro IPCA+ com Juros Semestrais 2035"
— same maturity, same bucket, one row survives. **The first proposed fix keyed on that bucket and
would have reintroduced the exact defect it was correcting.** Three independent reviewers found
this separately. It is preserved as a test:
`tests/test_keys.py:133 test_the_first_proposed_fix_would_have_been_worse_than_the_defect`.

Two more edges baked in:

- **The hash is computed exactly once**, in Polars, in the bronze landing
  (`ofl/ingestion/tesouro.py:58` via `keys.with_instrument_id`). Spark **never** recomputes it.
  A second implementation diverges on NULL alone: `a || NULL` propagates NULL in Spark and DuckDB,
  while `f"{...}|None"` in Python yields a perfectly non-null hash for a row with no maturity.
  `keys.instrument_id_expr:103` publishes the same pre-image as SQL so "the two languages agree"
  is a test (`tests/test_keys.py:92`, evaluated in DuckDB) rather than a claim.
- **A missing maturity raises.** It is reachable: the Tesouro extractor parses the column with
  `strict=False`. Every silent alternative is worse — a null id withholds the whole release, a
  coalesced id collapses every malformed row into one fictitious instrument. `tesouro.py:57` drops
  null `maturity` alongside null `date` before the hash is taken.

### 6.3 The unit trio — `unit` alone was never a unit

`unit ∈ {percent, brl, usd, index, contracts, none}` is not enough. The trio is
`unit` + `basis` + `scale`, plus `day_count` and `horizon` for anything compounded
(`ofl/registry.py:45-50`).

The worked example that the design got wrong on its first attempt, and that you will get wrong
too: **`selic` (SGS 11) is `basis: per_day`.** The *annualised* one is `over` (SGS 1178). Writing
`{"series_id": "selic", "unit": "percent", "basis": "per_year"}` stamps a daily rate as annual —
an agent then answers *"a Selic está em 0,05% ao ano"* with a `pass` contract and a green
freshness chip. Compare `sources/registry.yml:42` (`selic`, `per_day`) against `:70` (`over`).

Acceptance is **correctness, not presence**: `tests/test_units.py` diffs the trio against the
versioned golden `tests/fixtures/registry_units.expected.csv`, reading the **raw YAML** with
`yaml.safe_load` rather than the parsed `Series` object — because every field on `Series` has a
permissive default and "no `scale` declared" is unrepresentable once parsed.

Eleven series carry `unit_scope: per_column` instead; their units live in the table contract.

### 6.4 `avg` is wrong for a Copom month

The SELIC target for a month is the rate **in force on the last business day**, obtained with
`row_number() OVER (... ORDER BY date DESC) = 1` — the pattern `mart_macro_dashboard.sql` already
used. It is **never** the month's average.

A cut from 15.00 to 14.50 on the 18th averages to ~14.79% — a rate that was in force on **no day
of that month**. Publishing it is publishing a number that never existed.

See `mart_real_interest_exante.sql`, CTE `selic`. Same rule for the Focus expectation: last survey
with date ≤ month end, not the month's mean.

Related, same file family: **Fisher exact, never subtraction.** `(1+i)/(1+π) - 1`, not `i - π`. At
two-digit Brazilian rates the two conventions diverge materially and both are alive in the market,
so publishing without fixing one publishes an ambiguity (D11).

And `mart_real_interest_expost.sql` compounds the **daily** effective SELIC — product of `(1+r_d)`,
not a sum, not an average — which is only meaningful over a complete window. Hence
`n_business_days_expected` from `dim_date.is_business_day_br` published beside
`n_business_days_observed`, and `checks/assert_expost_window_is_complete.sql` turning the gap into
a failure rather than a footnote.

### 6.5 The percentile convention

`pct_rank = (n_below + 0.5 * n_ties) / n_obs` — mid-rank. DuckDB's `percent_rank()` is
`(rank-1)/(n-1)` and `cume_dist()` is `rank/n`, and all three get called "percentile" in the wild.
On a ten-year daily SELIC window with a long plateau they differ by ~**16 percentage points**.
`n_below` and `n_ties` are published so anyone can recompute the others
(`ofl/release/recipes/percentile_asof.sql`). D12's reversal test: swap in `cume_dist` and the
golden must fail.

Rank is **as-of** — a row is ranked only against observations up to its own date. Ranking against
full history would rewrite every past row on every release and the artefact would stop being
pinnable. What as-of does *not* buy is row immutability: the sources revise and the landing is
full-refresh (`land_bronze` writes `mode="overwrite"`, `conform_observations` does
`whenMatchedUpdateAll()`). **Immutability is of the release, not of the row.**

### 6.6 Two gaps found while writing this document

Both are static readings — Spark is not runnable here — so treat them as leads, not verdicts.

1. **`fact_observation` may never get its provenance.** `_FACT_OBSERVATION_DDL`
   (`silver.py:34`) declares `provider STRING, data_class STRING`, and `build_series_metrics`
   (`silver.py:389`) selects them. But `_read_bronze_observations` (`silver.py:39`) projects only
   `series_id, date, value, source, ingested_at, load_id` — **`provider` and `data_class` are not
   in the union**, so the MERGE source has no such columns. If that reading is right, silver
   `fact_observation` carries nulls in both, and every downstream `required`/`license`/`class`
   gate on a real (non-fixture) release fails or passes for the wrong reason. Confirm with a Spark
   run before changing anything.

2. **The adversarial-corpus test is missing one clause.** RFC F2/fixtures requires the committed
   corpus to contain "≥1 non-redistributable provider". `tests/test_release.py:49`
   `test_the_corpus_is_adversarial` asserts the NTN-B pair, two cadences and a short series — but
   not that. The corpus providers are `bacen_sgs`, `bacen_focus`, `tesouro_direto`, all open. The
   licence canary (`test_release.py:171`) synthesises a restricted row in-test instead, so the
   gate *is* proven; the *corpus* just is not the thing proving it.

---

## 7. The five verified defects in published code, and their status

§0 of the RFC. "Published" = `origin/main` @ `b21dfc0`. Check any of these with
`git show origin/main:<path>`.

| # | Defect | Where it was | Fixed? |
|---|---|---|---|
| **A** | `MERGE ON t.bond = s.bond AND t.date = s.date` + `partitionBy("bond","date")`, with `maturity` in the row and **outside the key**: rows differing only by maturity collide and one is dropped in silence | `ofl/transform/spark/silver.py:140,157` (published) | **fixed on this branch**, commit `9e54e01`. Key is now `keys.TREASURY_KEY = ("instrument_id","date")`; merge predicate built at `silver.py:186`; pre-dedup collision check at `silver.py:156`. *Spark path unverified by execution.* Residual: `dim_instrument_map` still missing (§2.2). |
| **B** | `mart_yield_curve.sql` did not project `source`. Tesouro (real) and ANBIMA (**sandbox, values fictitious by the provider's own description**) landed in one fact with no discriminator | `ofl/transform/gold/models/mart_yield_curve.sql` | **fixed**, commit `ad21a33`. The mart now projects `instrument_id, provider, data_class`. `land_bronze` (`landing.py:37`) takes `data_class` keyword-only **with no default**, so a handler that forgets to think about it fails rather than inheriting the permissive value. |
| **C** | `mart_real_interest.sql` declared itself "Ex-post" while dividing the month's SELIC **target** (forward-looking) by **trailing** realised IPCA — neither ex-ante nor ex-post | `ofl/transform/gold/models/mart_real_interest.sql:1` | **fixed**, commit `b2a943e`. Two honest marts born beside it. The old one **keeps its name** through the deprecation window and is relabelled DEPRECATED at line 1 — renaming would break `runner.py`'s `CHECKS` dict and three tests while buying a consumer nothing (D14's carve-out is about *wrong numbers*, not *schema churn*). |
| **D** | `dim_date` had neither business days nor holidays | `ofl/calendar.py` | **fixed before the RFC**, commit `65d38cb`. 23 tests: Easter 2023–26, every movable feast, the `from_year` cut, and a structural invariant over 11 years. |
| **E** | `unit` was not a unit — `percent` covered 22 incompatible series; 18 of 51 carried values with no destination | `sources/registry.yml` | **fixed**, commit `04d68e8`. Trio + `day_count` + `horizon`; golden CSV; test reads raw YAML. See §6.3. |

All five are fixed **on this branch and not published**. `origin/main` still carries all five.

---

## 8. Where decisions live, and the rule about reversal

**`E:/Projetos/Portifolio/_specs/DECISOES-OFL.md`** — D1 through D17. Every decision carries a
**razão** and a **critério de reversão**.

> **Reversão é teste, não opinião.**

A reversal criterion is not "revisit if it feels wrong". It is a command someone can run whose
outcome settles the question. Examples from the registry:

- **D12** (mid-rank percentile) — *"swap the recipe for `cume_dist` and the `golden` gate must
  fail."* That is executable today.
- **D15** (grain before dedup) — *"feed `dedup_latest` two rows differing only by `maturity`; it
  must raise, not return one."* That is `tests/test_keys.py:225`.
- **D1** (GitHub Releases, not R2) — *"reverts when the credential exists **and** a single file
  passes 2 GB, or the release passes 10 GB, or GitHub's raw rate limit blocks a real consumer."*
  Three measurable thresholds, not a preference.
- **D5** (coverage frozen, zero new series) — *"thaws when the public release is green in CI, and
  the first new series after that arrives with a named user, not with 'it was missing'."*

**If you are about to change a decision, find its D-number first and run its reversal test.** If
the test does not fail, the decision stands regardless of how the change feels. If a decision has
no runnable criterion, that is a defect in the registry and worth fixing before the change.

New decisions taken while implementing belong in that file with the same shape, not in a commit
message.

### The method note, which is the most transferable thing here

Two adversarial rounds each found blockers **in the previous round's corrections**. v3 reintroduced
the defect it was correcting (`instrument_id` keyed on `bond_type`); v4 corrected a gate that could
not fail (`grain` post-dedup).

> The proposed fix deserves the same scepticism as the defect — and the only sceptic that never
> gets tired is a test that fails.

D17 stopped the review at v4 because every remaining blocker had the same shape — *a gate verified
by reasoning rather than by execution* — and the correction for all of them is identical: build the
canary and watch it fail. D17's own reversal criterion: reverts if implementing F1/F2 reveals a
defect of **design** (not of code) that another round would have caught.

---

## 9. Suggested order of attack

Not prescriptive — this is what the measurements above imply, in rough dependency order.

1. **`dim_instrument_map`** (§2.2). The largest hole, and it is the one that makes C1 finished
   rather than mostly-finished. Needs: the table, `first_seen_release`, the fail-closed build
   behaviour on an unmapped label, and the acceptance test F1/C1(v).
2. **Wire `key_drift`** (§5). Needs a previous release's key set threaded into `build_release`. Do
   it together with (1) — they are the same alarm from two sides.
3. **Confirm or refute the `fact_observation` provenance gap** (§6.6.1). Needs a Spark run.
4. **Finish the release contents** — `latest.json`, `recipes/`, `golden/` (§2.2/P1). Making
   `golden` a real gate rather than a test falls out of this.
5. **`ofl release publish` CLI** (§2.2/P9) so `publish()` has a caller.
6. P3 / P6 / P7 marts and metadata, which are additive and blocked on nothing.

`docs/DEPLOY.md` covers what is left after all of the above: a repository and a token, both human
steps.
