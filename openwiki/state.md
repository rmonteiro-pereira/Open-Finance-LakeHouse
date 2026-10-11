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
| Public snapshot (`ofl publish`) | **deployed**, seeded by hand | #56, #58 | The target is a dedicated bucket on the lakehouse's own object store, the only one with anonymous read (objects only; listing, writing and the other buckets answer 403, checked from inside the cluster). First snapshot written by hand on 2026-10-09 from a throwaway pod: 8 marts. The DAGs with `publish_snapshot` after `ofl_gold` were published to the `deploy` branch on 2026-10-10. **Not verified:** the task on a scheduled cycle. `mart_yield_curve` and `mart_equity_daily` resolve to `private` and are not published (see `MART_INPUTS`). |

## Handoff

- **Unverified:** the new silver trigger on a real 03:00 UTC wave; whether 4 ingest
  slots are safe for the BACEN API (30 of 51 series share it).
- **Web dashboard:** the public face is `web/` (Next.js), which reads only the snapshot
  written by `ofl publish`. Built and looked at (desktop light and dark, phone) against a
  snapshot taken from the real lakehouse on 2026-10-09. Not deployed: the snapshot is not
  reachable from the internet yet. The cluster's gateway has a public route for the
  snapshot bucket alone (read-only), but it waits for a TLS certificate, a DNS record and a
  firewall rule, all on the owner's side; the route has never answered a real request.
  After that: a Vercel project on `web/` with `SNAPSHOT_URL`. The admin face (private
  network) is not built.
- **Worth doing here:** task timeouts; pin image builds to `uv.lock`; vacuum old Delta
  versions (each silver run leaves one behind, about 20 MB).
