# OFL Lakehouse — Dashboard Handoff

**Audience:** an agent (or engineer) building a BI dashboard on top of the
Open-Finance-LakeHouse. This is everything you need to **connect, query, and know
what's in the data** — without reading the pipeline source.

> TL;DR — The data is **Delta tables in MinIO** (S3-compatible), bucket
> **`lakehouse`**. The dashboard should read the **`gold/` marts** (ready-to-plot)
> and the **`silver/` star schema** (for custom cuts). Query them with **DuckDB**
> (`delta_scan`) or **Polars** (`read_delta`). Credentials come from the k8s
> secret `minio-creds` — they are **not** in this doc.

---

## 1. Architecture in one paragraph

Medallion lakehouse, engine-per-lane: **Polars** extracts public financial/macro
APIs → **bronze** (one Delta table per series); **Spark** conforms bronze →
**silver** star schema (idempotent Delta MERGE); **DuckDB** builds **gold** marts
from silver. Everything is driven by a metadata registry (`sources/registry.yml`,
48 series). All three layers are **Delta Lake** tables in MinIO. A dashboard only
needs the **silver** and **gold** layers.

```
public APIs ──Polars──▶ bronze/ ──Spark MERGE──▶ silver/ (star schema) ──DuckDB──▶ gold/ (marts)
                         (per series)              fact_* + dim_*                    mart_*
```

---

## 2. How to connect (ACCESS)

| Thing | Value |
|---|---|
| Object store | MinIO (S3-compatible) |
| **In-cluster endpoint** | `http://minio.minio.svc.cluster.local:9000` |
| Bucket | `lakehouse` |
| Table format | Delta Lake (delta-rs / Spark-written) |
| Region | `us-east-1` (nominal; MinIO ignores it) |
| URL style | **path** (required for MinIO) |
| TLS | none in-cluster (`USE_SSL false`) |
| **Credentials** | k8s secret **`minio-creds`** (namespace `default`), keys `MINIO_USER` / `MINIO_PASSWORD` |

**Getting credentials (do NOT hardcode):**
- If the dashboard runs **in-cluster** (recommended), mount the secret:
  `envFrom: [{ secretRef: { name: minio-creds } }]` → gives `MINIO_USER` /
  `MINIO_PASSWORD` as env. Also set `MINIO_ENDPOINT=http://minio.minio.svc.cluster.local:9000`.
- If running **outside the cluster**, you must expose MinIO (NodePort / ingress /
  `kubectl port-forward svc/minio -n minio 9000:9000`) and read the creds with
  `kubectl get secret minio-creds -n default -o jsonpath='{.data.MINIO_USER}' | base64 -d`
  (likewise `MINIO_PASSWORD`). The creds also live in OpenBao if a shared path is set.

### DuckDB — the one gotcha that matters

DuckDB's `delta_scan` reads S3 creds from the **secret manager** (`CREATE SECRET`),
**not** from the legacy `SET s3_*` session variables. Without the secret it falls
through to the AWS default chain and **hangs on the EC2 metadata endpoint**
(169.254.169.254) inside the cluster. Always register the secret:

```python
import os, duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs; INSTALL delta; LOAD delta;")
con.execute(f"""
    CREATE OR REPLACE SECRET minio (
        TYPE S3,
        KEY_ID  '{os.environ["MINIO_USER"]}',
        SECRET  '{os.environ["MINIO_PASSWORD"]}',
        ENDPOINT 'minio.minio.svc.cluster.local:9000',   -- host:port, NO scheme
        URL_STYLE 'path',
        USE_SSL  false,
        REGION   'us-east-1'
    )""")

# Ready-to-plot mart:
df = con.execute(
    "SELECT * FROM delta_scan('s3://lakehouse/gold/mart_macro_dashboard')"
).df()
```

> Shortcut: the `ofl` package already encapsulates this. If you `pip install` it
> (or run inside the `ghcr.io/rmonteiro-pereira/open-finance-lakehouse:slim`
> image), call `from ofl.transform.gold.runner import configure_minio; configure_minio(con)`
> and skip the boilerplate. It reads `MINIO_ENDPOINT/MINIO_USER/MINIO_PASSWORD` from env.

### Polars (alternative)

```python
import polars as pl
opts = {
    "AWS_ENDPOINT_URL": "http://minio.minio.svc.cluster.local:9000",
    "AWS_ACCESS_KEY_ID": MINIO_USER,
    "AWS_SECRET_ACCESS_KEY": MINIO_PASSWORD,
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}
df = pl.read_delta("s3://lakehouse/gold/mart_equity_daily", storage_options=opts)
```

---

## 3. Layout & a ⚠️ warning about legacy orphans

```
s3://lakehouse/
  bronze/<fact>/<series_key>      # raw-but-typed, one Delta table per series
  silver/<table>                  # conformed star schema  <-- build dashboards here
  gold/<mart>                     # BI-ready marts          <-- and here
```

⚠️ **The `lakehouse` bucket also contains LEGACY prefixes** from the old (Kedro)
pipeline that predates the current design: e.g. `bronze/bacen_selic`,
`bronze/yahoo_finance`, `gold/selic_kpis`, `gold/*_kpis`, `gold/focus_pib`. **Ignore
these.** The **current** layout is the one documented below: `silver/fact_*`,
`silver/dim_*`, `gold/mart_*`. If a name isn't in sections 4–5, it's legacy — don't use it.

---

## 4. Silver star schema (`s3://lakehouse/silver/<table>`)

Build custom views from these. Grain and columns:

### `fact_observation` — single-value time series (most macro series)
`series_id STRING, date DATE, value DOUBLE, source STRING, ingested_at TIMESTAMP, load_id STRING`
- Grain: one row per `(series_id, date)`. `source` = handler. Partitioned by `source`.
- Join `dim_series` on `series_id` for human names / domain / unit.

### `fact_treasury` — government bond prices & yields (Tesouro Direto + ANBIMA TPF)
`bond STRING, maturity DATE, date DATE, buy_rate DOUBLE, sell_rate DOUBLE, buy_price DOUBLE, sell_price DOUBLE, source STRING, ingested_at TIMESTAMP, load_id STRING`
- Grain: `(bond, date)`. `sell_rate` is the reference/indicative yield (use this for curves).

### `fact_security_price` — daily OHLCV (equities, ETFs, indices, FX, commodities)
`symbol STRING, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, source STRING, ingested_at TIMESTAMP, load_id STRING`
- Grain: `(symbol, date)`. Sources: `yahoo` (global + BR ETFs/commodities/FX), `b3` (Ibovespa index), `b3_cotahist` (official B3 cash-market equities).

### `dim_series` — business metadata for every series (the catalog as a table)
`series_id STRING, name STRING, domain STRING, source STRING, category STRING, unit STRING, frequency STRING, fact STRING`

### `dim_date` — calendar (1980–2035)
`date DATE, date_key INT, year INT, quarter INT, month INT, day INT, day_of_week INT, is_month_end BOOL`

### `series_metrics` — pre-computed pct-change + rolling stats over `fact_observation`
`series_id STRING, date DATE, value DOUBLE, pct_change DOUBLE, rolling_3_avg DOUBLE, rolling_12_avg DOUBLE, rolling_12_vol DOUBLE`

---

## 5. Gold marts (`s3://lakehouse/gold/<mart>`) — ready to plot

| Mart | Grain | Key columns | What it shows |
|---|---|---|---|
| **mart_macro_dashboard** | month | `month, selic_target, ipca_mom, usd_brl, debt_to_gdp_pct` | Wide monthly macro panel (policy rate, inflation, FX, gross debt/GDP). The natural landing page. |
| **mart_real_interest** | month | `month, selic_target, ipca_accum_12m, real_interest_rate` | Ex-post real interest (Selic deflated by 12m IPCA). |
| **mart_inflation_panel** | month | `month, ipca_mom, ipca15_mom, inpc_mom, igpm_mom, igpm_12m, igpdi_mom, ...` | All inflation indices side by side + 12m accumulation. |
| **mart_fx** | symbol × date | `series_id, date, rate, daily_return_pct, vol_21d, mtd_return_pct` | USD/BRL & EUR/BRL levels, returns, 21d realized vol. |
| **mart_yield_curve** | bond × date | `date, bond, maturity, years_to_maturity, yield, buy_rate, sell_price, bond_type` | Treasury yield curve over time (`bond_type` ∈ ipca_plus/prefixado/selic). |
| **mart_equity_daily** | symbol × date | `symbol, date, open, high, low, close, volume, daily_return_pct, sma_21, vol_21d, high_52w, low_52w` | Per-security daily analytics for all equities/indices/FX/commodities. |

> Marts are full overwrites each gold run. They're small — safe to load entirely
> into the dashboard process and slice client-side.

---

## 6. Series catalog (48 series)

`domain` → `series_id` (what it is). Query `dim_series` for the live list. All land
in `fact_observation` unless marked **[T]** (fact_treasury) or **[S]** (fact_security_price).

- **rates:** `selic` (Selic over, daily), `cdi` (CDI daily), `over` (Selic annualized),
  `selic_meta` (Copom target), `tlp`, `cdi_anual` (CDI annualized base-252), `tr`,
  `poupanca`, `focus_selic_fim_ano` (Focus survey: Selic, end-of-year median).
- **inflation:** `ipca`, `ipca_15`, `inpc`, `igp_m`, `igp_di`, `igp_10`, `ipc_fipe`,
  `ipca_nucleo_ms`/`_ma`/`_dp`/`_ex3` (IPCA cores), `focus_ipca_12m` (Focus: 12m-ahead IPCA).
- **fx:** `usd_brl` (PTAX sell), `eur_brl`, `usd_brl_compra` (PTAX buy), `focus_cambio_fim_ano`.
- **fiscal:** `divida_pib` (**gross** DBGG %GDP), `dlsp_pib` (**net** DLSP %GDP),
  `ibc_br` (activity index, GDP proxy), `resultado_primario` (R$ mn), `reservas_internacionais`
  (US$ mn), `ipea_nfsp_primario`, `ipea_divida_liquida`, `ipea_pib`.
- **credit:** `credito_total` (R$ mn), `inadimplencia_pf` (household NPL %), `inadimplencia_pj` (firm NPL %).
- **market:** `tesouro_direto` **[T]**, `anbima` **[T]** (secondary-market TPF, sandbox),
  `anbima_ima_b`/`anbima_ima_b5`/`anbima_irf_m` (ANBIMA IMA index levels),
  `b3` **[S]** (Ibovespa index via Yahoo), `ibge` (unemployment %).
- **equities [S]:** `yahoo_etf`, `yahoo_commodity`, `yahoo_currency`, `yahoo_global`
  (S&P/Nasdaq/VIX/US10Y/DXY/Brent/EUR-USD/USD-MXN), `b3_cotahist` (official B3 cash-market OHLCV).

**Notes / caveats**
- BRL macro series are floored at the **Plano Real (1994-07-01)**; no pre-1994 history.
- ANBIMA series (`anbima*`) run against the **sandbox** → values are *fictitious but
  format-real*. Fine for demos; not real market data.
- `value` units differ by series (percent, R$ million, index, BRL) — always read
  `dim_series.unit`; don't mix units on one axis.
- Annualized rates (`cdi_anual`, `over`) legitimately reach ~173% in 1994-97.

---

## 7. Suggested dashboard pages (low effort, high signal)

1. **Macro overview** — `mart_macro_dashboard` (Selic target, IPCA, USD/BRL, debt/GDP) as KPI tiles + lines.
2. **Real interest** — `mart_real_interest` (nominal vs real) + the Focus *ex-ante* read (`focus_selic_fim_ano` − `focus_ipca_12m`).
3. **Inflation** — `mart_inflation_panel` (headline vs cores vs IGP family).
4. **FX** — `mart_fx` (USD/BRL level + 21d vol) with global context from `mart_equity_daily` (DXY, ^TNX).
5. **Yield curve** — `mart_yield_curve` (animate `yield` vs `years_to_maturity` over `date`; color by `bond_type`).
6. **Equities** — `mart_equity_daily` (blue-chip watchlist: close, 21d vol, 52w range).

---

## 8. Refresh / ops

The pipeline runs on the cluster via **Airflow 3 DAGs** (`ofl_ingest_<handler>` →
`ofl_silver` → `ofl_gold`, data-aware Assets). Marts refresh automatically when
upstream bronze updates. A dashboard just re-reads the Delta tables — no coupling
to the pipeline. To force a rebuild manually: run the `:slim`/`:spark` images with
`ofl ingest` / `ofl silver` / `ofl gold` (env: `MINIO_*` + the bucket default `lakehouse`).
