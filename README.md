# Open-Finance LakeHouse

A single-node, GitOps-managed **lakehouse for Brazilian macro & financial data**
(BACEN/SGS rates & indices, IBGE, IPEA, Tesouro Direto, Yahoo/B3, ANBIMA).

> **Polars extracts → Spark refines → DuckDB serves.** One engine per lane, driven
> by a single source registry. See [`docs/architecture/redesign.md`](docs/architecture/redesign.md)
> for the full rationale.

This is deliberately **small data** — no engine here is load-bearing for volume.
Each is chosen as the right tool for its lane (and the strongest showcase).

## Architecture

| Lane | Engine | Output |
|------|--------|--------|
| extract → `bronze` | **Polars** | one Delta table per series (windowed, idempotent, contract-checked) |
| `bronze` → `silver` | **Spark + Delta** | conformed star schema via idempotent `MERGE` (`fact_observation`, `fact_treasury`, `dim_series`, `dim_date`, `series_metrics`) |
| `silver` → `gold` | **DuckDB** | SQL marts (real interest, inflation panel, FX, macro dashboard, yield curve) |

Orchestrated by **Airflow 3 data-aware Asset DAGs** (per-domain ingest →
asset-triggered silver → gold). Quality gates are **Pandera** contracts at ingest;
lineage is **OpenLineage → OpenMetadata**. Everything — ingestion, DAG generation,
dimensions, the catalog — is driven from [`sources/registry.yml`](sources/registry.yml).

## Layout

```
ofl/                      # the package
  config.py               # pydantic settings (env-driven)
  registry.py             # typed loader for sources/registry.yml
  platform/               # spark session, MinIO/Delta IO, logging, lineage
  ingestion/              # Polars extractors per source family
  transform/spark/        # silver: conform, MERGE, window KPIs
  transform/gold/         # DuckDB SQL marts + runner
  quality/                # pandera contracts
sources/registry.yml      # metadata that drives everything
orchestration/airflow/    # per-domain + asset-driven DAGs
docker/                   # offline cluster images (:slim, :spark) — see docker/README.md
tests/  docs/architecture/
```

## Quickstart

```bash
uv sync                       # install (extras: .[spark,airflow,yahoo,lineage,dev])

ofl registry                  # list the registered series
ofl ingest --series selic     # one series → bronze (Polars)
ofl ingest                    # all active series → bronze
ofl silver                    # bronze → silver (Spark MERGE + dimensions)
ofl gold                      # silver → gold marts (DuckDB)
```

Configuration is environment-driven (`MINIO_ENDPOINT`, `MINIO_USER`,
`MINIO_PASSWORD`, `LAKEHOUSE_BUCKET`, `AWS_REGION`, `OFL_REGISTRY`). On the cluster
the lanes run as `KubernetesPodOperator` pods using the prebuilt offline images
(`docker/`): `:slim` for Polars ingest + DuckDB gold, `:spark` for the silver MERGE.

## Tests

```bash
uv run pytest
```

## Notes

- **Plano Real floor (1994-07-01):** BACEN/SGS ingestion is floored at the Real via
  a registry `start_date` default — pre-Real cruzeiro/hyperinflation data is
  economically incomparable and trips the quality contracts. Per-series overridable.
- **ANBIMA** is implemented but `status: planned` until Feed API credentials are
  provided (`ANBIMA_CLIENT_ID` / `ANBIMA_CLIENT_SECRET`).
