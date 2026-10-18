# OFL adversarial product review — 2026-10-18

## Verdict and scope

OFL has a more valuable data foundation than the recently added agents expose.
The implementation was organized around source categories and infrastructure
safety before establishing user outcomes. The previous 20-feature list was
premature as a product roadmap. It remains an ideas inventory, not evidence of
what finance workers, data workers, assessores, consultores or lay portfolio users
need most.

The strongest direction to test is **a trusted Brazilian financial research and
data workbench**: connect economic data, market context, news and manager views;
help people understand and challenge claims; produce reproducible datasets,
analysis and strategy-research specifications. This is a product hypothesis, not
validated demand or proof that OFL beats alternatives.

Finance and data workers are both primary domains. Professional and lay investment
portfolio evaluation belong in product discovery. The engineering/career portfolio
project is a separate secondary objective. Those two meanings of “portfolio” must
not be conflated.

Method: inspect merged source at `abcb973`, read deployed aggregate metadata and
raw-store inventories, exercise six deterministic counterexamples through the
actual Inkwell edge, and check relevant primary-source product documentation.
No user interviews, live-generation-quality trials, competitor task races, rights
clearance or customer-adoption study were performed. No runtime change or production
write was made for this evaluation.

## What OFL actually has

| Surface | Evidence checked | What it does and does not establish |
| --- | --- | --- |
| Public/core macro lane | Current snapshot: 37 published observation series, 97,101 rows, 8 marts; snapshot built 2026-10-18 | A substantial usable starting collection; not uniform completeness, correct semantics of every series or historical information availability |
| Letters collection | 4,183 stored PDF objects, 36 manager manifests; collector classifies those PDFs as stored letters | A potentially valuable research collection; not manually verified content, independent opinions, performance records or cleared redistribution |
| Letter dates | 3,470 stored manifest records have a publication date; 713 do not | Date-sensitive research must qualify/exclude unresolved dates; upload/collection time cannot substitute |
| News collection | 3,078 JSONL batch objects and 29 source-state objects; latest batch object write checked | Existing collection activity; batch objects are not unique-article counts or full-text availability |
| New private research index | One quote trading date, 2026-01-05; zero catalog items, active assets/passages, reviewed observations or OCR candidates | New archive/market workflows are not yet supported by broad indexed coverage. This is not evidence that all OFL documents are absent |
| Agent access | Tools for private quotes/observations, private archive search/items, coverage and operations | No access to the core macro snapshot or raw letters/news collections |
| Agent policy | One planning step; actual values/text excluded from model observations; model prose replaced deterministically | Preserves a useful safety boundary but prevents the intended interpretive/knowledge workflow |
| Existing dashboard/API | Snapshot publication, catalog, downloads, private queries/reviews, pinned exports | Useful engineering and data-serving assets. They are not proof of persona fit or repeat use |

Exact aggregate evidence and qualifications: [inventory](evidence/inventory-2026-10-18.json).
The old core lane includes futures-derived marts. The newer all-market COTAHIST
scope excludes futures. Those facts should be explained rather than silently
changing the previously agreed source scope.

## Persona ranking and its limitations

[Twenty candidate personas](personas.json) include finance research, investment
advice/consulting, corporate treasury, risk/credit, financial journalism, data
engineering, analytics/BI, systematic research, quant engineering, retrieval/NLP,
API builders, stewardship and lay market/portfolio evaluation. Education and
archive partners are additional adjacent audiences. Hiring/technical reviewers
have a separate engineering-portfolio evaluation.

The [reproducible ranking](evidence/persona-ranking.md) uses pain (25%), recurrence
(15%), cross-source value (20%), current-data fit (15%), hypothetical pilot reach
(10%), differentiation (10%) and responsible delivery feasibility (5%). Ratings are
author judgments without interview evidence. A second profile emphasizes near-term
data fit. Scores that are close are tied in practical meaning; they are not precise
estimates of market demand, ROI, willingness to pay or user counts.

The leading cohorts are financial data engineers and macro/multi-asset analysts,
followed by investment research, analytics/BI and assessores. This supports paired
finance/data pilot tracks. It does not justify dropping consultants or lay users:
they require explicit comprehension/portfolio-context tests and a simpler entry
point, not merely a lower rank on a professional research interface.

## Adversarial findings

### P0 — The agent omits OFL's strongest existing assets

`agents/src/agents.ts` offers no core macro, news or manager-letter tools. Meanwhile
thousands of PDFs and news batches and 37 macro series are already stored/published.
The recently built archive specialist queries an empty new passage catalog.

Consequence: the project pays the complexity of an agent without exposing the
most plausible cross-source user value. Read the whole product graph before
expanding a source-specific subsystem. Next experiment: complete one macro/news/
letters brief and one versioned dataset workflow using bounded existing inputs.

### P0 — Source retrieval was mistaken for completed research

`observe()` sends counts and a generic limitation, not retrieved passages or values.
`evidencePlanner()` replaces every no-tool answer with a generic request for fields.
The edge permits one planning step. A dependent search → read-context sequence
cannot complete in that turn. Tailored goal clarification and basic knowledge
explanations are discarded even if the underlying model produced them.

Reproduced diagnostic AE01–AE04: four structural gaps, not four measured customer
failures. AE05–AE06 confirm exact source values/provenance and read-only tools.
The correct response is a bounded evidence-aware reasoning/writing/calculation
workflow, not the removal of safeguards. Selecting a better model cannot fix
these policy and tool-access restrictions by itself.

### P0 — No complete outcome definition or adoption evidence

The last iteration proved code, ownership, transport and container behavior. It
did not show a finance worker completing a brief, an assessor explaining a client
portfolio, a layperson understanding exposures, or a data worker reproducing a
dataset faster or better than their current method. “205 tests passed” is useful
implementation evidence and is not product validation.

Next experiment: paired artifacts against the real workaround and OFL without an
agent. Record correctness and verification work as well as time. Keep an efficient
API/file/notebook workflow if an agent adds no value for a data-worker task.

### P0 — Strategy support needs information-time and financial semantics

Pinned storage releases are useful; they do not reconstruct everything a researcher
could have known on a historical date. Current revised macro data, unknown letter
publication dates, instrument continuity, adjustment factors, historical universe
membership and trade-cost assumptions constrain what a backtest can establish.

Support exploratory/descriptive analysis and strategy specification with explicit
qualification now. Promote a performance-backtest workflow only when its required
inputs and validation protocol exist. Distinguish a research hypothesis from an
observed correlation, a causal conclusion, or a deployable profitable strategy.
The [ALFRED real-time API](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)
is a concrete reference for distinguishing current-history values from information
known at a past date; it does not prove OFL already has equivalent Brazilian vintages.

### P1 — Technical categories and codes leak into the user journey

A financial analyst, assessor or leigo generally starts with a company, theme,
question or portfolio—not “market 010,” an item ID, review-candidate ID or ingestion
job. Data workers need technical detail, but in context: definitions, schema,
coverage, lineage and transformations.

Prefer an investigation/data question entry point, goal-based modes, progressive
resolution of identities/dates and inspectable assumptions. Keep quality operations
available to data operators/stewards rather than treating operations as a universal
third customer-facing assistant. Do not remove professional detail from the data
workbench merely to simplify the lay-user view.

### P1 — News and letters are the likely bridge, but require epistemic discipline

The collection is worth testing as a dedicated workflow: “What changed, what are
managers saying, where do they disagree, what do the measured data support, and
what would contradict this reading?” This can serve analysts, assessores,
consultores and lay users with different presentation depth.

Do not count repeated syndicated articles as independent corroboration. Preserve
author, document period, publication precision, ingestion time, topic and the exact
supporting passage. A manager's claim is an attributed opinion, not a verified
price, macro figure or performance result. Avoid deriving a stance from a missing
mention. The 713 undated stored records need resolution or explicit limitations.
Rights/tier metadata must remain attached when joining source collections; a
headline-only source must not become full-text content through a new interface.

### P1 — Coverage and freshness can look stronger than the job permits

`coverage()` counts years with any trading date; one date makes a year “present.”
This does not certify the requested period. Public snapshot status compares each
series against snapshot generation time; a frozen old snapshot can still describe
observations as fresh relative to that old build. The frontend also requires three
headline marts, so a missing one can prevent access to otherwise available data.
These are code-level failure modes, not a claim that the currently checked fresh
snapshot was frozen or inaccessible.

Separate source freshness, observation age, data cadence, pipeline success,
publication age and coverage of this exact task. Validate catalog frequencies and
units against the source before using automatic joins. Display missing/partial
coverage before analysis, not only in an operations page.

### P1 — Saved chat is not an immutable research or data package

The UI saves conversations and copies question/reference fragments for handoff.
The handoff caps references and clips the question at 8,000 characters. It does
not prove a complete, validated context transfer. Inkwell conversations can be
edited/retried/deleted, so describing them as immutable is inaccurate.

A defensible artifact needs source/data release IDs, relevant excerpts, exact
inputs, methods/code, scope, assumptions, information cutoff, qualifications,
model/version where used and a revision history. Provide notebook/Parquet/schema
outputs for data users and reviewable explanations for finance/lay users; JSON
chat dumps alone do not finish either job.

### P1 — API/MCP and “financial workspace” are not differentiation alone

[brapi documents financial data APIs and MCP](https://brapi.dev/docs/mcp).
[OpenBB documents a configurable data/research workspace](https://docs.openbb.co/).
Their documented existence disproves a claim that adding those interfaces is
itself unique. It does not establish equal coverage, permissions, historical
fidelity, pricing or real workflow quality.

Test a narrower advantage: Brazilian source integration, explicit semantics,
reproducibility, evidence/counterevidence and lower verification effort on a
specific task. If direct sources plus notebooks or an existing service win,
integrate with them or narrow the job rather than adding another generic layer.

### P1 — Distribution and source scope need an intentional audience choice

Tailscale is sensible for a private pilot; it creates friction for public discovery
or external collaborators. Do not build a new public authentication/business system
before a pilot shows demand. The public app can demonstrate a read-only, cleared
sample workflow while private work retains source/client controls.

The repository name “Open Finance” also overlaps the Brazilian banking-data-sharing
term used by [BCB](https://www.bcb.gov.br/estabilidadefinanceira/cliente-open-finance).
This creates a positioning hypothesis to test, not a mandatory rename. Make the
financial research/data-work purpose understandable before stack descriptions.

### P2 — Architecture and documentation lead the presentation

README and public hero lead with Polars/Spark/DuckDB/Kubernetes and registered
series, rather than a user question and result. Some wiki overview claims predate
image publishing and the research lane. Long technical explanations belong behind
a demonstrated useful workflow. Maintain dated evidence and remove claims that
mistake registered assets for published accessible data.

The engineering foundation is a strength: source identity, exact raw prices,
immutable originals/releases, audited reviews, reproducible snapshots, private
boundaries, API/export and failure-oriented tests. Preserve it; make its payoff
visible in tasks rather than rebuilding the stack for presentation value.

## Candidate product workflows and resulting agent shape

1. **Understand and challenge a market narrative.** Select a theme/period or ask in
   plain language. Reconcile macro data with dated news and manager commentary.
   Return findings, opposing evidence, unknowns and the next research question.
   Assessor/consultant and lay modes simplify presentation without weakening evidence.
2. **Build an eligible financial dataset.** Discover coverage and definitions;
   choose fields/frequency/cutoff; validate identities/units; apply approved
   transforms; export a pinned dataset and a runnable Python/SQL recipe. The agent
   may guide the work; a direct API/BI/notebook route can be the best interface.
3. **Evaluate a portfolio question or strategy hypothesis.** Establish holdings,
   weights, objectives/horizon and required data, or explicitly identify their
   absence. Explain descriptive exposures or propose a reproducible experiment.
   Compute only through tested methods. Do not claim portfolio performance,
   personal suitability or a backtest when inputs do not support it.

A news/letters research capability should be an internal specialist or a focused
workflow within these journeys, not another disconnected source-tab bot. Useful
internal roles are scope/eligibility, retrieval/context, calculations, skeptical
challenge, writing and evidence verification. Inkwell remains the runtime candidate;
its adoption does not define the product boundaries.

The output contract is question, scope/as-of/coverage, key findings, sources,
method, counterevidence, uncertainty and next action, with linked tables/charts or
data/code artifacts when useful. Explanation, numerical validity, retrieval
quality and finished-task quality need separate grading.

## Direction and sequence

**First:** rerank with actual work examples and benchmark two leading finance/data
jobs, including advisor/consultant and lay comprehension checks. Audit a bounded
letters/news subset for dates, extraction, duplicates and use conditions.

**Then:** connect that subset and the existing macro snapshot to a shared catalog
and eligibility/lineage layer. Complete one brief and one reproducible dataset
workflow before expanding the number of specialist personas exposed in UI.

**Then:** bounded adaptive retrieval, approved calculations, evidence-bound synthesis,
contradictory-source handling, saved artifact versions and relevant updates. Add
portfolio/context inputs and strategy-test capabilities as their data contracts
become valid. Do not make a model endpoint the definition of “done.”

**Expand only after measured value:** wider history, more instruments, additional
source adapters, knowledge/monitoring workflows, collaboration or specialized
interfaces justified by actual task failures. Reassess every inherited backlog
item against persona, job, corpus, metric and reversal condition.

The engineering-portfolio track should demonstrate the same real end-to-end value
with public-safe data, a runnable setup, adversarial evaluations, reproducibility,
resource measurements and honest limits. Its value is stronger when it shows
product judgment as well as infrastructure skill.
