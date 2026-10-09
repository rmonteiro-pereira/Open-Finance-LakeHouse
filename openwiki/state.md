---
workstream: OFL
repo: this repo (Open-Finance-LakeHouse, public)
branch: main
phase: OFL-deploy
status: done
updated: 2026-10-08T12:30Z
---

> Written from a read of `main` at `d61ea70` and from deploying it on a new
> single-node cluster (2026-10-07). Earlier history of this project is in its commits
> and merged PRs, not repeated here.

## Ledger

| Phase | Status | Evidence | Note |
|---|---|---|---|
| Runtime requirements mapped | **done** | `d61ea70` | Images, env vars, secrets, pools, RBAC, endpoints: see `architecture.md`. |
| Runs on Ceph RGW | **verified** | cluster run, 2026-10-07 | Ingest and silver completed against Ceph S3 with no code change. |
| Full pipeline on the new cluster | **verified** | Airflow `dag_run`, 2026-10-08 | Ingest, silver and gold all succeeded (gold three times). Bucket after six silver runs: 124 MB, 463 objects. Still failing: `anbima` (no credentials). |
| Silver once per ingest wave | **done**, not yet seen on a scheduled wave | #44 | `ingest_done` markers; `OFL_SILVER_TRIGGER=any` restores the old trigger. |
| `ibge` repaired | **done** | #45 | Upstream route was down; now table 6381, variable 4099, 174 months. |
| `b3_cotahist` rolling years | **done** | #48 | Also filters per file, so the memory peak is one annual archive. |
| Image build in CI | **partly** | #47, #49 | Builds both images on PRs and main. Push to GHCR is off until the package grants this repo access and `PUSH_IMAGES=true` is set. |
| Public snapshot (`ofl publish`) | **done**, never run against the real lakehouse | this PR | Unit-tested on in-memory tables and a local target only. No bucket or Secret exists yet, so the DAG task is off. `mart_yield_curve` and `mart_equity_daily` resolve to `private` and are not published (see `MART_INPUTS`). |

## Handoff

- **Unverified:** the new silver trigger on a real 03:00 UTC wave; whether 4 ingest
  slots are safe for the BACEN API (30 of 51 series share it).
- **Web dashboard (branch `feat/web-dashboard`):** public face is a site that reads the
  snapshot; admin face stays on the private network. Neither is built yet.
- **Worth doing here:** task timeouts; pin image builds to `uv.lock`; vacuum old Delta
  versions (each silver run leaves one behind, about 20 MB).
