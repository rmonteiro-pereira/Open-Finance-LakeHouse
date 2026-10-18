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

- **Private research implementation (2026-10-16):** isolated file-first lane in
  `ofl/research`, separate Next app in `web/research`, PostgreSQL schema/jobs/reviews,
  exact pgvector baseline and a qualified local Qdrant pilot. See
  `docs/RESEARCH.md` and `docs/research/DECISIONS.md`. COTAHIST preserves all fields
  and non-futures markets. Existing equity/futures tasks are not migrated.
  Verified locally against disposable PostgreSQL/pgvector, synthetic COTAHIST,
  real OCR execution on a synthetic scanned PDF, and browser flows. The pinned
  multilingual model retrieves a synthetic OCR passage by paraphrase. This is
  **not** real-source coverage, a retrieval-quality benchmark or a production
  deployment. Real input inventory, compatibility cutover, private gateway rollout,
  capacity measurements and final backend selection remain unverified.

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

## OFL Inkwell research agents (implementation branch, 2026-10-18)

`agents/` adds market/archive/quality specialists using approved private Inkwell
packages, PostgreSQL conversations and separate specialist schemas. Private
packages are ignored and must never be published with this public repo. Read-only
tools call the existing private API; monetary values stay strings. One planning
call delivers source evidence tables without claiming a verified model narrative.
The private dashboard adds Pesquisa, saved investigations, evidence links,
specialist handoff and Markdown/JSON exports.

REST adds private OpenAPI discovery, storage-path-free release listings and
read-only per-principal bearer access. `ofl research mcp` provides five bounded
stdio read tools. Gateway and exact-origin checks remain in place.

Local verification covers Inkwell tool/ownership/security tests, a disposable
PostgreSQL restart/ownership test, actual stdio MCP initialization, backend tests
and a production research dashboard build. Live rollout and generation-model
validation remain pending; existing BGE-M3 is embeddings-only. This does not claim
the 20-item backlog or full historical/archive ingestion is complete.

The local agent image also bootstrapped against disposable PostgreSQL and rejected
unauthenticated requests (401). Browser fixtures passed query → exact source table
→ save/reopen → JSON export → specialist handoff and rendered on mobile. Model
planning prose is replaced deterministically so an invented reading cannot be
presented as financial evidence. Final local totals: 202 Python passes / 3 optional
skips and 9 Inkwell passes. The root Docker context excludes the private agent tree.
