# OFL container images

Two images, one per engine lane — built so the cluster (whose pods have **no
external egress**) can run every step **offline**.

| Image | Dockerfile | Lane | Runtime | `ofl` cmds |
|-------|-----------|------|---------|------------|
| `:slim`  | `docker/Dockerfile`       | extract + serve | Python only           | `ingest`, `gold`, `registry` |
| `:spark` | `docker/Dockerfile.spark` | refine          | Python + JRE 17       | `silver` |

## Why two images
- **Slim** runs Polars ingest and the DuckDB gold marts — no JVM, so it stays
  small. The DuckDB `httpfs`/`delta` extensions are **pre-baked** into
  `/opt/duckdb-ext` and loaded offline via `DUCKDB_EXTENSION_DIRECTORY`
  (`ofl/transform/gold/runner.configure_minio`).
- **Spark** runs the silver conform/MERGE. The Delta + `hadoop-aws` jars are
  **baked into `pyspark/jars`** so `OFL_SPARK_JARS_PACKAGED=1` skips Ivy
  resolution (which would need network).

## Build

```bash
# from the repo root (build context = repo root; see .dockerignore)
docker build -f docker/Dockerfile       -t ghcr.io/rmonteiro-pereira/open-finance-lakehouse:slim  .
docker build -f docker/Dockerfile.spark -t ghcr.io/rmonteiro-pereira/open-finance-lakehouse:spark .
```

## Run (local smoke test)

```bash
# registry listing — needs nothing external
docker run --rm ghcr.io/rmonteiro-pereira/open-finance-lakehouse:slim registry

# gold against a reachable MinIO
docker run --rm \
  -e MINIO_ENDPOINT=http://host.docker.internal:9000 \
  -e MINIO_USER=... -e MINIO_PASSWORD=... -e LAKEHOUSE_BUCKET=lakehouse \
  ghcr.io/rmonteiro-pereira/open-finance-lakehouse:slim gold --dry-run
```

## Runtime configuration (env)
`MINIO_ENDPOINT`, `MINIO_USER`, `MINIO_PASSWORD`, `LAKEHOUSE_BUCKET`,
`AWS_REGION`, `OFL_REGISTRY` (defaults to the baked `/app/sources/registry.yml`),
`OFL_SPARK_JARS_PACKAGED` (slim: unset; spark: `1`),
`DUCKDB_EXTENSION_DIRECTORY` (slim: baked to `/opt/duckdb-ext`).

## Cluster note
The Airflow Asset DAGs (`orchestration/airflow/dags/ofl_dags.py`) launch these via
`KubernetesPodOperator`: domain ingest + gold on `:slim`, silver on `:spark`.
Ingest still needs an **egress path to the source APIs** (BACEN/IPEA/Yahoo/
Tesouro) — that is a cluster NetworkPolicy/DNS concern, not an image one.
