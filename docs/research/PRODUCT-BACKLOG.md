# OFL research product backlog

Product review note (2026-10-18): this list is an unvalidated feature inventory,
not a selected product roadmap. The whole-OFL [evaluation workflow](../product/WORKFLOW.md)
and [adversarial review](../product/REVIEW.md) take precedence for prioritization.
Finance/data workers, assessores/consultores and lay market/portfolio users need
outcome validation; source-specialist implementation tests do not establish that.

Scope: the private OFL research backend and dashboard. This ledger distinguishes
implemented capabilities from remaining work; it is not a deployment claim.

| # | Improvement | State after Inkwell integration |
| --- | --- | --- |
| 1 | Overview of coverage, gaps, reviews and failures | Existing coverage/operations; unified overview pending |
| 2 | Grounded research agent | Three Inkwell specialists return source evidence tables; live generation endpoint pending |
| 3 | Editable scope before execution | Required quote tool fields and existing market filters; explicit agent scope editor pending |
| 4 | Saved investigations | PostgreSQL conversations per owner/specialist, reopen in dashboard; workspace annotations pending |
| 5 | Identity-aware instrument watchlists | Pending; ticker equality must not imply continuity |
| 6 | Exact price comparisons | Existing exact decimal/basis query; guarded comparison workflow pending |
| 7 | Immutable release comparisons | Release discovery added; record-level diff workflow pending |
| 8 | Explainable semantic search | Existing hybrid results retain model/revision/method; rank contribution UI pending |
| 9 | Evidence viewer/bundles | Existing originals/pages plus agent item/passage/asset/page links and evidence exports; consolidated viewer pending |
| 10 | Readable and inspection views | Research evidence tables and existing operations/review views; explicit display toggle pending |
| 11 | OCR review history | Existing versioned reviews; quality specialist can inspect within reviewer permissions |
| 12 | Processing timeline | Existing attempts/progress/retry operations and specialist inspection; visual timeline pending |
| 13 | Coverage monitoring | Existing recorded coverage; saved watch rules and gap detection pending |
| 14 | Historical date qualifications | Tools label publication intervals versus quote dates; unknown/approximate timeline UI pending |
| 15 | Guided investigation workflow | Question → source tables → saved conversation/export; reviewable finding/brief stages pending |
| 16 | Specialist handoff | Market/archive/quality handoff carries question and source references into a new investigation |
| 17 | Finding cards | Pending; distinguish source observation from interpretation and limitations |
| 18 | Research exports | Markdown and JSON evidence exports; reviewed narrative brief template pending |
| 19 | Diagnostic feedback | Pending classification, review and evaluation-candidate workflow |
| 20 | Saved research checks | Saved conversations retain tool results but can be revised/deleted; immutable artifact and versioned rerun comparisons pending |

Additional interfaces: existing authenticated REST extended with private OpenAPI
and release discovery, plus issued read-only service credentials. Stdio MCP
provides five bounded read tools. No gateway secret is distributed to MCP clients.

Remaining work must retain the dedicated research database, source validation,
private original files, release identities, exact decimal prices, quotation basis,
user ownership and trusted private gateway. No public snapshot app proxy is added.
