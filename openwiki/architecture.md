# Open-Finance-LakeHouse — architecture

## Digest
- **Medallion, one engine per lane:**

  | lane | engine | output |
  |---|---|---|
  | extract to `bronze` | Polars (delta-rs) | one Delta table per series, idempotent, contract-checked |
  | `bronze` to `silver` | Spark + Delta, local mode | star schema via idempotent `MERGE` |
  | `silver` to `gold` | DuckDB | SQL marts with post-build checks |

- **`sources/registry.yml` drives everything:** ingestion, DAG generation and
  `dim_series`. A new BACEN/SGS series is one entry; a new kind of source is one
  handler in `ofl/ingestion/`.
- **Orchestration** (`orchestration/airflow/dags/ofl_dags.py`): one `ofl_ingest_<handler>`
  DAG per handler, `@daily`, one task per series, each emitting
  `Asset("lakehouse://bronze/<series>")`. Each DAG ends with `ingest_done`
  (`all_done`), emitting `Asset("lakehouse://ingest/<handler>")`. `ofl_silver` triggers
  when **all** ten of those have fired, so once per ingest wave; `ofl_gold` on the
  silver asset. `ofl_backfill` is manual.
- **Two images**, built from the repo root: `docker/Dockerfile` (`:slim`, ingest and
  gold) and `docker/Dockerfile.spark` (`:spark`, silver, JRE 17 plus baked jars).
  Entrypoint is the `ofl` CLI: `ofl ingest --series <key>`, `ofl silver`, `ofl gold`.
- **Config is env-only** (`ofl/config.py`): `MINIO_ENDPOINT`, `MINIO_USER`,
  `MINIO_PASSWORD`, `LAKEHOUSE_BUCKET`, `AWS_REGION`, `OFL_REGISTRY`,
  `OFL_SPARK_DRIVER_MEMORY`. Pods get them from one Secret (default name
  `minio-creds`) via `envFrom`.
- **Public snapshot** (`ofl/publish.py`, `ofl publish`): the cluster accepts no inbound
  traffic, so the pipeline pushes outward. One Parquet per gold mart, the open series,
  `catalog.json` and a `manifest.json` with checksums go to an S3-compatible bucket under
  `snapshots/<run_id>/`; `latest.json` moves last. What may leave is decided by
  `redistribution` in the registry (`open`, `derived`, `private`; a mart takes the most
  restrictive tier of its inputs in `MART_INPUTS`). `ofl_gold` gets a `publish_snapshot`
  task only when Airflow has `OFL_PUBLISH_SECRET` (the Secret holding `OFL_PUBLISH_*`).
- **No catalog service.** Delta tables on S3 are the catalog. Postgres and Redis are
  needed only by Airflow. OpenLineage and the Pushgateway are optional, env-gated.

## What the deployment must provide
- The Secret must contain **`MINIO_ENDPOINT`**: the DAG does not inject it and the
  default is `http://localhost:9000`.
- Credentials equal to `minioadmin` are rejected for any non-loopback endpoint.
- The bucket is **never created by this code**. Create `lakehouse` first.
- Airflow pools **`ofl_ingest`** and **`ofl_spark`** must exist; the DAGs do not
  create them. The slot counts (2 and 1) appear only in comments.
- Airflow components must be able to `import ofl` and read `sources/registry.yml` at
  parse time (a repo checkout on `PYTHONPATH` works).
- The launcher's service account needs pod create/get/list/watch/delete and
  `pods/log` in the namespace the pods run in (`OFL_NAMESPACE`, default `default`).
- Path-style S3 over plain HTTP is assumed (S3A has `ssl.enabled=false` hard-coded).

## Key decisions and why
- **Pools instead of locks.** There is no lock provider for Delta on S3
  (`AWS_S3_ALLOW_UNSAFE_RENAME=true`); the pools and `max_active_runs=1` are what keep
  one writer per table.
- **Spark in local mode inside one pod**, 4g driver heap in a 6Gi limit: the data is
  small, and a cluster would cost more than it returns.
- **Images carry everything they need at run time** (DuckDB extensions, Spark jars),
  so pods need no egress except the data sources themselves.
