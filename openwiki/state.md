---
workstream: OFL
repo: this repo (Open-Finance-LakeHouse, public)
branch: docs/openwiki
phase: OFL-deploy
status: running
updated: 2026-10-08T02:24Z
---

> Written from a read of `main` at `d61ea70` and from deploying it on a new
> single-node cluster (2026-10-07). Earlier history of this project is in its commits
> and merged PRs, not repeated here.

## Ledger

| Phase | Status | Evidence | Note |
|---|---|---|---|
| Runtime requirements mapped | **done** | `d61ea70` | Images, env vars, secrets, pools, RBAC, endpoints: see `architecture.md`. |
| Runs on Ceph RGW | **verified** | cluster run, 2026-10-07 | Ingest and silver completed against Ceph S3 with no code change. |
| First full daily cycle on the new cluster | **running** | Airflow `dag_run` table | 30 of 51 ingests succeeded and one silver run completed at last check; gold not yet seen to finish. |
| Image publishing in CI | not started | — | Images are built on the node and imported by hand. |

## Handoff

- **Unverified:** a completed gold run on the new cluster; why `ibge` fails there;
  whether 4 ingest slots are safe for the BACEN API (30 of 51 series share it).
- **Worth doing here:** make `ofl_silver` run once after the ingests instead of per
  asset batch; add timeouts; make `b3_cotahist` years follow the calendar; pin image
  builds to `uv.lock`; a CI job that builds and pushes both images.
