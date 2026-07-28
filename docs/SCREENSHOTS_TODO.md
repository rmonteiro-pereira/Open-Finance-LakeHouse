# Screenshots to capture

The README sells the architecture in text. Screenshots prove it **runs**. This is the shot list —
capture each one from the live cluster, save it under `docs/img/` with the exact filename below,
then embed it in the README section noted in the last column.

Nothing here is captured automatically: these come from running services (Airflow, MinIO,
OpenMetadata, Grafana), so a human has to take them.

## Before you start

- **Redact secrets.** Blur or crop anything showing credentials, tokens, access keys, the
  `.env` contents, internal IPs, or the DuckDNS hostname if you'd rather not publish it.
  MinIO's console shows the access key in the top bar — crop it out.
- **Use a wide window** (~1600px) and light-on-dark or the tool's default theme, consistently.
  Mixed themes across shots look sloppy.
- **Prefer a populated state.** A DAG grid with only one green run is less convincing than a
  week of history; a mart preview with 10 rows beats an empty table.
- **PNG, trimmed.** Crop to the interesting rectangle — no full desktop, no browser chrome/tabs
  with unrelated sites. Aim for < 500 KB each so the repo stays light.
- Create the folder on first use: `docs/img/`.

## Shot list

| # | Screenshot | Where to capture it | Filename (`docs/img/…`) | Why it earns its place | README section |
|---|---|---|---|---|---|
| 1 | **Airflow DAG list** — all `ofl_ingest_<handler>` DAGs plus `ofl_silver`, `ofl_gold`, `ofl_backfill`, filtered by the `ofl` tag | Airflow UI → DAGs, filter tag `ofl` | `airflow-dags-list.png` | Shows the registry actually generates one DAG per source handler | Architecture › Orchestration topology |
| 2 | **Airflow grid view with run history** — pick the busiest ingest DAG (`ofl_ingest_bacen_sgs`) and show its per-series task columns across several days of runs | Airflow UI → DAG → Grid | `airflow-grid-bacen-sgs.png` | This is the per-series isolation claim, visible: one task (and one asset) per series | Architecture › Orchestration topology |
| 3 | **Airflow Assets view** — the `lakehouse://bronze/<series>` assets and the asset-triggered `ofl_silver` → `ofl_gold` edges | Airflow UI → Assets (or the DAG's graph showing inlets/outlets) | `airflow-assets-graph.png` | Proves data-aware scheduling rather than cron-chaining | Architecture › Orchestration topology |
| 4 | **MinIO bucket layout** — the `lakehouse` bucket showing `bronze/`, `silver/`, `gold/` prefixes; ideally drill one level into `silver/` so the `fact_*` / `dim_*` tables are visible | MinIO Console → Object Browser → `lakehouse` | `minio-lakehouse-buckets.png` | Makes the medallion layout concrete and shows real objects landed | Architecture › Storage layout |
| 5 | **A Delta table's file layout** — inside e.g. `silver/fact_observation`, showing `_delta_log/` next to the parquet parts (and the `source=…` partitions) | MinIO Console → drill into `silver/fact_observation` | `minio-delta-table-detail.png` | Evidence it's genuinely Delta, partitioned — not parquet dumped in a folder | Architecture › Storage layout |
| 6 | **OpenMetadata lineage** — the bronze → silver → gold lineage graph for one table (`fact_observation` or `mart_macro_dashboard`) | OpenMetadata → Explore → table → Lineage tab | `openmetadata-lineage.png` | The OpenLineage integration is the hardest thing to believe from text alone | Tech stack (Lineage / catalog) |
| 7 | **A gold mart query + result** — run `SELECT * FROM delta_scan('s3://lakehouse/gold/mart_macro_dashboard') ORDER BY month DESC LIMIT 20` in a DuckDB CLI/notebook cell and capture the query with its result table | Notebook or terminal (`duckdb` after `configure_minio`) | `duckdb-mart-macro-dashboard.png` | Closes the loop: real numbers out the serving end, queryable in one line | Data coverage › Gold marts |
| 8 | **Grafana / Alertmanager per-series observability** — a panel over `ofl_series_last_success_timestamp_seconds` (freshness by series) or the alert list showing per-series rules | Grafana → dashboard, or Alertmanager UI | `grafana-series-freshness.png` | Backs the "per-series alerting" claim with an actual panel | Operations & reliability |

### Optional extras (nice, not required)

- **A rendered chart from a mart** — DI futures curve from `mart_futures_curve`, or the real-interest
  series from `mart_real_interest` → `docs/img/chart-real-interest.png`. Good hero image for the top
  of the README.
- **`ofl registry` terminal output** — cheap to capture, shows the metadata-driven design in one
  frame → `docs/img/cli-ofl-registry.png`.

## Embedding

Once the files exist, drop them into the README with a caption:

```markdown
![Airflow generates one ingest DAG per source handler, each with one task per series](docs/img/airflow-grid-bacen-sgs.png)
*One task, one bronze Asset, one alert per series — a failing source withholds only its own data.*
```

Alt text is not optional — it's what a recruiter's screen reader (and GitHub's image-failed
fallback) shows. Describe what the screenshot *demonstrates*, not just what it is.

## Checklist

- [ ] `docs/img/` created
- [ ] 1 — `airflow-dags-list.png`
- [ ] 2 — `airflow-grid-bacen-sgs.png`
- [ ] 3 — `airflow-assets-graph.png`
- [ ] 4 — `minio-lakehouse-buckets.png`
- [ ] 5 — `minio-delta-table-detail.png`
- [ ] 6 — `openmetadata-lineage.png`
- [ ] 7 — `duckdb-mart-macro-dashboard.png`
- [ ] 8 — `grafana-series-freshness.png`
- [ ] All shots reviewed for leaked credentials / internal hostnames
- [ ] Embedded in the README with descriptive alt text
