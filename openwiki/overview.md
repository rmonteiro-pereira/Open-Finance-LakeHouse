# Open-Finance-LakeHouse — overview

## Digest
- **What:** a lakehouse for Brazilian macro and financial data: BACEN/SGS rates and
  indices, BACEN Focus, IBGE, IPEA, Tesouro Direto, B3 files, Yahoo, ANBIMA.
  Deliberately **small data**; no engine here is load-bearing for volume.
- **Slogan:** *Polars extracts, Spark refines, DuckDB serves.* One engine per lane,
  everything driven by `sources/registry.yml`.
- **Scale**, from the registry, never hand-counted: **10 handlers, 51 active series,
  51 bronze assets, 13 DAGs** (10 ingest, `ofl_silver`, `ofl_gold`, `ofl_backfill`).
- **Where it runs:** as `KubernetesPodOperator` pods launched by Airflow 3 on a
  single-node Kubernetes cluster, writing Delta tables to S3. The cluster, Airflow
  values and secrets live in the private `Infra-lakehouse` repo, not here. This repo
  has **no Kubernetes manifests, Helm values or image-publishing CI**.
- **Storage:** one bucket, `lakehouse`, prefixes `bronze/`, `silver/`, `gold/`. Any
  S3-compatible store works (no MinIO-only calls); it ran on MinIO and now runs on
  Ceph RGW. The env vars are still named `MINIO_*`.
- **Open:** no CI builds or publishes the two images; `ibge` ingest fails in the
  current deployment (cause not investigated); the 4 `anbima` series need API
  credentials; `ofl_silver` re-runs on every batch of bronze assets.
