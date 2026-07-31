# Gold export — lakehouse marts as a portable DuckDB file

`tools/export_gold_duckdb.py` copies the **gold marts** out of MinIO into a single
local DuckDB database plus one parquet file per mart. It is a **read-only** consumer
of the lakehouse: it does not build the marts, and it never writes to the bucket.

> TL;DR — `python tools/export_gold_duckdb.py` → `../_artifacts/ofl_gold.duckdb`
> with the 8 `mart_*` tables inside. The artifact lives **outside the repo** and is
> never committed.

---

## 1. What it does

For each mart in `ofl.transform.gold.runner.MODELS` it runs
`delta_scan('s3://<bucket>/gold/<mart>')` and materializes the result as a local
table, then writes the same rows to a parquet sibling:

```
_artifacts/
├── ofl_gold.duckdb          # all marts as tables
├── mart_equity_daily.parquet
├── mart_futures_curve.parquet
├── mart_fx.parquet
├── mart_inflation_panel.parquet
├── mart_macro_dashboard.parquet
├── mart_open_interest.parquet
├── mart_real_interest.parquet
└── mart_yield_curve.parquet
```

The database also carries a small `_export_manifest` table — one row per mart with
`status` (`ok` / `empty` / `missing`), `rows`, `source_uri` and `exported_at`. It
lets a consumer tell a genuinely empty mart from one that failed to export. Nothing
is ever synthesized: a mart that is missing upstream is reported as `missing` and no
table is created for it.

**Re-runnable.** Tables are written with `CREATE OR REPLACE TABLE` and parquet files
are overwritten, so a second run is indistinguishable from a first one. Re-run it
whenever you want a fresher snapshot.

---

## 2. Read-only guarantee

The `lakehouse` bucket is treated as immutable production storage:

- The only object-store operation is `delta_scan` — a **reader**.
- The only writes are to the local `--out-dir`.
- No `PUT`/`DELETE`, no bucket policy, lifecycle or admin (`mc admin`) calls.
- The gold transformation is **not** re-run; the script copies what the pipeline
  already materialized under `gold/`. To rebuild the marts you want `ofl gold`,
  which is a different tool with different (write) semantics.

Credentials are read through the repo's existing config (`ofl.config.Settings`) and
are never printed or logged — the console output shows the endpoint host and bucket
only.

---

## 3. What the artifact is for

A downstream text-to-SQL agent answers business questions by querying the gold marts.
It must **not** reach into the cluster to do so — no VPN, no k8s credentials, no
dependency on the homelab being up. This export is that boundary: the agent opens
`ofl_gold.duckdb` locally and queries it like any other database.

The parquet siblings exist for consumers that would rather not speak DuckDB
(Polars, pandas, Spark, DuckDB-WASM in a browser).

---

## 4. How to run it

Configuration comes from the environment / `.env`, same as the rest of the project
(see `ofl/config.py`): `MINIO_ENDPOINT`, `MINIO_USER`, `MINIO_PASSWORD`,
`AWS_REGION`, `LAKEHOUSE_BUCKET`. Nothing is hardcoded in the script.

```bash
# from the repo root, with the project venv active
python tools/export_gold_duckdb.py
```

Useful flags:

| Flag | Effect |
|---|---|
| `--out-dir PATH` | where to write (default: `<repo parent>/_artifacts`) |
| `--marts mart_fx mart_yield_curve` | export a subset |
| `--no-parquet` | DuckDB file only |
| `--strict` | exit non-zero if any requested mart is missing |

Exit code is `1` if no mart produced rows (or, with `--strict`, if any mart is
missing); `0` otherwise.

### Running from outside the cluster

MinIO is not reachable at a plain `localhost` by default. Either use the ingress or
port-forward, and take the credentials from the k8s secret (never hardcode them):

```bash
# option A — via the MinIO API ingress
export MINIO_ENDPOINT=https://minio-api.vanir.dev.br

# option B — port-forward (read-only, no cluster mutation)
kubectl port-forward svc/minio -n minio 9000:9000 &
export MINIO_ENDPOINT=http://localhost:9000

export MINIO_USER=$(kubectl get secret minio-secrets -n minio -o jsonpath='{.data.root-user}' | base64 -d)
export MINIO_PASSWORD=$(kubectl get secret minio-secrets -n minio -o jsonpath='{.data.root-password}' | base64 -d)

python tools/export_gold_duckdb.py
```

If a checkout's `.env` still points at an older MinIO hostname, the environment
variable above wins — `.env` is the fallback, not the override.

### Troubleshooting

| Symptom | Cause |
|---|---|
| Every mart reports `missing` with `SignatureDoesNotMatch` (403) | The credentials are stale, not the path. MinIO answers a wrong secret key with a signature error rather than `AccessDenied`. Re-export `MINIO_PASSWORD` from `minio-secrets` as above. |
| Every mart reports `missing` with a connection error | `MINIO_ENDPOINT` is unreachable. The API ingress serves **https** and 308-redirects plain http, which S3 request signing does not survive — use `https://minio-api.vanir.dev.br`. |
| A single mart reports `missing` | That mart was never materialized under `gold/`. Run the gold transformation (`ofl gold`) first; the export never invents rows. |

---

## 5. Verifying the artifact

```python
import duckdb

con = duckdb.connect("../_artifacts/ofl_gold.duckdb", read_only=True)
con.execute("SHOW TABLES").fetchall()
con.execute("SELECT mart, status, rows FROM _export_manifest ORDER BY mart").fetchall()
```

A healthy export, measured on 2026-07-29 against `https://minio-api.vanir.dev.br`
(8/8 marts `ok`):

| mart | rows |
|---|---:|
| `mart_real_interest` | 327 |
| `mart_inflation_panel` | 384 |
| `mart_macro_dashboard` | 384 |
| `mart_fx` | 14,918 |
| `mart_open_interest` | 22,374 |
| `mart_yield_curve` | 33,574 |
| `mart_equity_daily` | 158,764 |
| `mart_futures_curve` | 1,621,818 |

≈38 MB of DuckDB plus ≈24 MB of parquet. Row counts grow with each pipeline run —
treat these as an order-of-magnitude sanity check, not a fixture. (A DuckDB file
never shrinks in place, so a database reused across many runs can be noticeably
larger than a freshly created one holding the same rows.)

---

## 6. Why the artifact is not in the repo

It is data, it is tens of megabytes, and it is a snapshot of a system that keeps
running. `_artifacts/` deliberately sits **outside** the repository; only this
document and the script are versioned. Rebuild the artifact by re-running the
script — that is the reproducible part.

For the schema of each mart and the wider lakehouse layout, see
[`DASHBOARD_HANDOFF.md`](DASHBOARD_HANDOFF.md).
