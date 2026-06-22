# Open-Finance LakeHouse — Modern Redesign

**Status:** active redesign (supersedes the Kedro-based implementation under `src/open_finance_lakehouse/`)
**Owner:** Rodrigo Monteiro Pereira
**Last updated:** 2026-06-19

---

## 1. What we are building (and why)

A **single-node, GitOps-managed lakehouse** for Brazilian macro & financial data
(24 series: BACEN/SGS rates & indices, IBGE, ANBIMA, B3, Tesouro Direto, IPEA,
Yahoo Finance). Two goals, stated honestly:

- **Goal A — a real analytical product.** Unify the series so cross-series
  questions become trivial: *real interest rate, yield curve, FX-adjusted
  returns, inflation panel.* Today this is impossible — gold is 24 isolated
  monthly-average tables.
- **Goal B — a portfolio that proves modern data-engineering range.** Idiomatic,
  hands-on **Spark, Polars, and DuckDB**, plus a current toolchain (no Kedro)
  that reads like 2026.

### The scale truth that shapes every decision
This is **small data** — full SELIC daily history ≈ 10k rows; all 24 series ≈ low
millions of rows / hundreds of MB as Parquet. **No engine here is required by
data volume.** Each engine is chosen because it is the right tool for its lane —
which is also the strongest showcase narrative.

---

## 2. Headline architecture: one engine per lane

> **Polars extracts → Spark refines → DuckDB serves.**

| Lane | Engine | Why this engine |
|---|---|---|
| **Extract → `raw` / `bronze`** | **Polars** | API pulls are I/O-bound, single-machine. Polars is fast, low-RAM, excellent at date/number parsing & reshaping. Protects the node's scarce 33 GB. |
| **`bronze` → `silver`** (conform, dedupe, upsert) | **Spark + Delta** | The data-engineering showcase: idempotent `MERGE`, schema enforcement/evolution, partitioning, `OPTIMIZE`/Z-ORDER, window functions. Spark owns a whole layer, used well. |
| **`silver` → `gold`** (marts + serving) | **DuckDB** | Query-on-the-lake: reads Delta/Parquet straight from MinIO; sub-second SQL for star-schema marts and BI. |

This mirrors how real modern stacks split responsibilities (ingestion runtime →
transformation engine → serving/query engine). Each tool earns its place; none is
load-bearing for scale.

---

## 3. Replacing Kedro

Kedro is retired. Each thing it provided gets a decoupled, modern replacement:

| Kedro provided | Replacement |
|---|---|
| Pipeline / node DAG | **Airflow 3 Assets** (data-aware orchestration) + typed Python tasks |
| Data catalog (`catalog.yml`) | **Delta tables on MinIO** (catalog of record) + **OpenMetadata** |
| Config loader (`conf/base`, params) | **`sources/registry.yml`** (single metadata-driven source registry) + **Pydantic settings** |
| `kedro viz` / lineage | **OpenLineage → OpenMetadata** (also delivers column-level lineage) |

**Gold transform framework:** intentionally **none for now** — gold marts are a
handful of DuckDB SQL files run by a thin runner. `dbt`/`SQLMesh` were evaluated
and deferred; the only material thing they'd add at this scale is auto column
lineage, which OpenLineage already covers. Revisit if the mart count grows.

---

## 4. Metadata-driven, not copy-pasted

The old design = 24 near-identical pipeline folders (each re-implementing
ingest/bronze/silver/gold). The new design = **one source registry drives
everything**: ingestion, DAG generation, dimensions, and the catalog.

`sources/registry.yml` is the single source of truth. Adding a BACEN series is a
one-entry change; a new *kind* of source adds one handler.

---

## 5. Data model

### Silver — conformed facts + dimensions (Spark / Delta)
- **`silver.fact_observation`** — grain `(series_id, date) → value`. **All
  single-value macro series unioned into one canonical long table** (SELIC, CDI,
  IPCA, IGP family, FX, dívida/PIB, reservas, …). The keystone that makes
  cross-series analysis trivial. Partitioned by `source` (+ `year` if needed).
- **`silver.fact_security_price`** — OHLCV grain for Yahoo / B3
  (`symbol, date, open, high, low, close, volume`).
- **`silver.fact_treasury`** — Tesouro Direto by `(bond, maturity, date)`
  (buy/sell rate + price).
- **Dimensions:** `dim_series` (name, source, category, unit, frequency, code),
  `dim_date` (calendar), `dim_security` (symbol, asset class, currency).

### Gold — marts that justify the lakehouse (DuckDB)
- `mart_real_interest` — SELIC/CDI vs IPCA/IGP → ex-post real rate.
- `mart_yield_curve` — Tesouro rates by maturity over time.
- `mart_fx` — USD/BRL, EUR/BRL daily + returns, rolling vol, MoM/YoY.
- `mart_inflation_panel` — IPCA / IPCA-15 / INPC / IGP family side-by-side, YoY.
- `mart_macro_dashboard` — wide monthly panel for BI.

All marts use window functions (returns, rolling vol, YoY) — showcased in SQL.

---

## 6. Orchestration topology (kills the "one DAG" problem)

Airflow 3 **data-aware** scheduling — not one mega-DAG, not 24 to babysit:

```
dag_ingest_rates ─┐
dag_ingest_inflation ─┤
dag_ingest_fx ─┤   6 per-domain ingestion DAGs (scheduled)
dag_ingest_fiscal ─┤        │ emit bronze Assets
dag_ingest_market ─┤        ▼
dag_ingest_equities ─┘  dag_silver_conform   (Spark; triggered by bronze Assets)
                                 │ emit silver Assets
                                 ▼
                        dag_gold_marts        (DuckDB; triggered by silver Assets)
```

Domains: **rates, inflation, fx, fiscal, market, equities.** `ofl_backfill`
remains as a manual "full rebuild" button.

---

## 7. Cross-cutting

- **Data quality:** retire Great Expectations → **Pandera** contracts at ingest
  (Polars-native) + SQL assertions at the gold layer. Lighter, in-repo,
  code-reviewed.
- **Lineage / catalog:** OpenLineage events from Airflow → OpenMetadata
  (column-level lineage; closes the old follow-up).
- **Idempotency:** Delta `MERGE` upserts + per-source watermarks → reruns are
  safe; no more full-refetch-and-overwrite.
- **Secrets:** kill hardcoded creds in `conf/base/spark.yml`; env / sealed-secret
  only.

---

## 8. Target repo layout (Kedro gone, casing bug gone)

```
open-finance-lakehouse/
  pyproject.toml            # uv-managed, no Kedro
  sources/registry.yml      # metadata that drives everything
  ofl/                      # the package (lowercase; no TitleCase bug)
    config.py               # pydantic settings (env)
    registry.py             # typed loader for sources/registry.yml
    platform/               # spark session, MinIO/Delta IO, logging, lineage
    ingestion/              # Polars extractors per source family
    transform/spark/        # silver: conform, MERGE, window KPIs
    transform/gold/         # DuckDB SQL marts + runner
    quality/                # pandera contracts
  orchestration/airflow/    # per-domain + asset-driven DAGs (generated)
  tests/  docs/architecture/
```

---

## 9. Migration path (incremental, never big-bang)

1. **Skeleton** — uv project + `sources/registry.yml` + `ofl/` platform, beside
   the old `src/`.  ← *in progress*
2. **rates domain end-to-end** — Polars→bronze, Spark→silver
   (`fact_observation` + dims), DuckDB→`mart_real_interest`. Prove parity vs.
   current MinIO.
3. **Airflow Assets** for that domain; retire its old pieces.
4. **Roll out** remaining 5 domains; wire OpenLineage; retire Kedro + GX.
5. **Polish** — OPTIMIZE/Z-ORDER schedule, time-travel demo, docs, tests.

The cluster keeps running the existing `:cluster` image until cut-over; nothing
in production breaks during the migration.

---

## 10. Decisions on record

- **Keep Delta** (not Iceberg) — OpenMetadata already catalogs it; less churn.
- **Spark stays** — chosen for the silver lane and as a deliberate showcase, not
  because data volume demands it.
- **No dbt/SQLMesh yet** — deferred; OpenLineage covers the lineage gap.
- **Three engines is deliberate** — this is a learning showcase; each engine maps
  to a real responsibility.
