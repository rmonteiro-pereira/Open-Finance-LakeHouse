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

CI follow-up: specialist switches reset state in their action handler rather than
synchronously in a React effect. The optional research/MCP tests skip cleanly in
the core-only test lane. Web lint ignores the private research build output, so
running lint after a production build does not inspect generated bundles.

## Whole-product adversarial review (2026-10-18)

`docs/product/` records a repeatable workflow, 20 persona hypotheses, weighted
ranking/sensitivity, 24 planned outcome cases and an evidence-backed initial review.
Finance workers and data workers are primary; assessores, consultores and lay
market/investment-portfolio users are explicit cohorts. Engineering portfolio
reviewers are secondary and must not be confused with investment portfolio users.
No interviews, adoption findings or live-model quality measurements were invented.

Read-only deployed metadata found 37 published macro series/97,101 observations,
8 public marts, 4,183 collector-classified stored letter PDFs across 36 manifests,
and 3,078 news JSONL batch objects. Batch/PDF counts are not independently verified
article/opinion counts. 713 stored letter records lack publication dates. The new
private research index still has one quote date (2026-01-05) and zero active archive
passages/items/observations; this does not mean all OFL documents are absent.

The current agent cannot access core macro data or the separate raw letters/news.
`agents/evals/product-audit.ts` exercised the actual edge with synthetic sources
and scripted models: four structural gaps (knowledge/scoping prose suppression,
no dependent read after search, disconnected upstream assets) and two preserved
safeguards (exact source values/provenance and read-only tools). These are diagnostic
counterexamples, not real user-task or generation-model success-rate measurements.
No runtime feature changes or production writes were made. The inherited backlog
is labeled unvalidated; its inaccurate immutable-conversation claim is corrected.


## Workbench foundation — 2026-10-18

Product shape documented in `docs/product/SHAPE.md`: financial research, data
exploration and portfolio understanding, with finance/data workers both primary.
Initial implementation connects existing news/letter objects and validated macro
snapshots to private API, MCP, Inkwell tools and research/data dashboard journeys.
See `docs/research/WORKBENCH.md` for bounded sync jobs and export semantics.
Local checks: 26 PostgreSQL-backed workbench/agent-API tests passed; private Next
build passed; lint has two existing warnings and no errors; nine existing Inkwell
tests passed before the additional macro-tool contract test. Production deployment,
real-source bridge acceptance and user outcome gates are not established by these
checks. Generative synthesis and portfolio holdings analysis remain subsequent
work; text generation still requires a configured provider.

Final local foundation verification: 222 Python tests passed, three optional
Qdrant/OCR checks skipped; TypeScript typecheck and ten Inkwell tests passed.
Chromium acceptance with simulated API fixtures covered data discovery/query,
version-pinned CSV links, evidence search/passage inspection and mobile overflow.
Private Next build passed and ESLint reported no errors (two existing warnings).
Python fatal/syntax Ruff rules passed; the full broad Ruff policy is not clean in
this lane and is not claimed as a passing check. Live-source acceptance and rollout
remain outstanding.

## Sequential product workflows — 2026-10-18

User authorized all remaining shape/backlog stages sequentially. Added owned
workspace revisions/findings, snapshot-pinned multiseries preparation, descriptive
valued portfolios/scenarios, complete-contract price comparisons, scoped release
diffs, identity watchlists, scheduled checks with owner revalidation, prospective
job events and diagnostic triage/history. Private dashboard exposes these journeys;
MCP now has eleven read-only tools. Inkwell permits six model steps/twelve tool
calls and independently re-reads cited passages rather than trusting user-side
model observations. Exact excerpts and draft interpretations are labeled distinctly.

Local: PostgreSQL product contracts, thirteen Inkwell tests, Next build/lint and
Chromium with real local API+database/synthetic inputs. The original diagnostic
artifact remains intact; sequential diagnostic is a separate file. Live macro and
one letter registered on an isolated disposable schema; no production pointer
changed. News rerun/live rollout and model acceptance remain separately tracked.
No claim of participant validation or historical completeness follows from this.

Real-source bridge rerun passed with the shipped news policy file: snapshot
20261018T040612Z, 38 open catalog entries/37 with data, two sampled series rows,
one news batch/two news items, one original letter. The isolated schema was
removed; no production corpus/pointer changed. Evidence is sanitized in
`docs/product/evidence/source-bridge-sequential.json`.
