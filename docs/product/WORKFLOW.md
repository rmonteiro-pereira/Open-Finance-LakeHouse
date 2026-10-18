# OFL product evaluation and direction-setting workflow

Status: created 2026-10-18. First evidence-backed desk audit completed; user
research, paired outcome trials and live generation evaluation are not completed.
The primary domains are **finance workers and data workers**. Assessores,
consultores and laypeople evaluating markets/investment portfolios are explicitly
included. The **engineering portfolio project** is a secondary objective; it is
not the same as a user's investment portfolio.

## Product question and decision rule

Which users can repeatedly produce a better, more trustworthy research or data
artifact with OFL than with their current tools, at an acceptable effort and cost?

Do not maximize features, source count, agent count, prompt traffic or stack size.
Optimize completed useful tasks, correctness, reproducibility and voluntary reuse.
Keep the financial-analysis and data-work jobs visible on the same trusted data
foundation; neither group is an afterthought. Treat knowledge-building and strategy
research as outcomes to prove, not labels attached to retrieval.

Every review produces an evidence ledger, persona ranking, task outcomes,
contradictions, proposed decisions, rejected alternatives and an updated roadmap.
A capability is separately labeled **implemented**, **deployed**, **data available**,
**task usable**, or **user validated**. Passing implementation tests proves only
some of those states.

## 1. Establish the facts and falsifiable promises

Owner: product/technical lead. Inputs: code at a recorded commit, live aggregate
inventory, access paths, source definitions and current operating constraints.

Map the entire chain: collection → catalog/identity → transformations → available
data → API/notebook/dashboard → agent → user's finished artifact. Audit macro,
market, letters/news, archival, public snapshot and private research lanes.
Do not infer the entire product from one database or worktree.

Record observation dates, publication/availability dates, ingestion times and
snapshot/release identifiers separately. Distinguish raw stored files, extracted
text, approved figures and source-backed analysis. A successful pipeline or a
recent snapshot does not certify every input's freshness, meaning or completeness.

Output: a dated inventory with source, method, scope and unknowns. Gate: every
proposed flagship task has a coverage/access/meaning check; missing data is explicit.
No private endpoints, credentials, client holdings or copyrighted document bodies
belong in public review artifacts.

## 2. Enumerate audiences before selecting an initial cohort

Use `personas.json`, not the three source-oriented specialist names. It covers 20
candidate personas across finance, data and market understanding, plus adjacent
source/education partners and separate portfolio-project reviewers.

For each: specify triggering situation, job, frequency, existing workaround,
required artifact, data dependencies, technical literacy, who verifies the result,
who decides to adopt, and a reason they would reject OFL. Rank with explicit
hypothesized criteria, then rerank under different weights. Never call those
scores observed demand, willingness to pay, market size or validation.

Reproduce the ranking: `python3 tools/rank_product_personas.py`.

Select two initial pilot tracks: one finance/market-interpretation workflow and
one data-delivery workflow. Include assessor/consultant and lay-user comprehension
checks; do not force everyone through a professional analyst's terminology.
Keep strategy researchers in discovery even when current data cannot support a
credible performance backtest.

## 3. Observe real work and benchmark the alternatives

Planned study, not already conducted: recruit by the selected role profiles, with
two finance researchers, two data workers, one assessor, one consultant and two
lay market/portfolio users for an initial eight-person exploratory round. Recruit
by role and experience rather than inventing named customers. Roles can overlap;
report the actual sample. This sample is for finding problems, not proving market size. Do not contact people without an authorized outreach task.

Ask participants to show a recent real task, its inputs, final deliverable, time
spent, verification steps, failure points and tools they actually used. Ask about
an actual past incident rather than whether an imagined feature sounds useful.
Separate user, data operator and adopter/buyer. Keep client information private.

Benchmark the same task and information cutoff against: (a) their actual workflow;
(b) direct source + spreadsheet/notebook; (c) OFL data/UI without an agent;
(d) OFL with the agent. Where relevant add an existing market API/MCP or research
workspace. Use the same source permissions, required artifact and evidence rules.
This separates OFL data value from agent value. Vendor documentation establishes
advertised capability only; hands-on results are required for superiority claims.

Output: task recordings/notes with consent, artifacts, observed work and switching
conditions. Gate: at least one painful recurring job in each primary domain has
actual examples; otherwise revisit targeting before expanding the feature list.

## 4. Define small complete workflows and their data contracts

Start with the three candidate journeys in REVIEW.md and cases.json:

- Research/interpret: macro data + news + manager views → supported findings,
  counterevidence and a reviewable brief.
- Data work: a question → eligible, versioned dataset → validated transformation
  → an independently reproducible Python/SQL/BI consumption recipe.
- Market/portfolio understanding: clarify holdings/objective or explicitly state
  they are missing → explain relevant exposures and competing narratives in plain
  language → cite evidence and preserve uncertainty.

Choose a bounded period/topic/source set from existing assets. A complete small
workflow is more informative than ingesting every historical file first. Preserve
the longer historical/archival collection as an expansion path.

Each contract lists source coverage, definitions, units, frequency/calendar,
identity continuity, transformations, date semantics, access/use constraints,
required versus optional fields, expected missingness and permissible conclusions.
A stored manager letter is opinion with an author/date; it is not verified alpha.
A headline is not its article. Collection time is not historical availability.

Output: coverage manifest and expected artifact for each task. Gate: the minimum
corpus can answer the task, and the unsupported parts can be identified honestly.
Unknown source-use permissions require checking the specific source before broader
redistribution; this workflow does not assert legal clearance.

## 5. Evaluate the agent as a workflow, not a chat demonstration

Use one investigation-facing entry point with different task/complexity modes;
source specialists can remain internal. Prototype the smallest sequence that can
clarify → check eligibility → plan → retrieve → inspect context → compute through
approved deterministic methods → seek counterevidence → synthesize → verify → save.

Finance workers need interpretation, comparisons and strategy hypotheses. Data
workers need query/coverage/schema/lineage/export and reproducibility support.
Assessores/consultores need reviewable client explanations. Laypeople need
progressive disclosure, definitions and goal-oriented questions rather than raw
market codes. All modes must share the same evidence, not different truth rules.

Retain Inkwell, exact values, ownership and source boundaries. Evaluate its writer,
evidence binding and bounded multi-step capabilities before replacing the runtime.
Do not fix today's blanket prose suppression merely by permitting unchecked model
arithmetic or uncited financial claims. Tools produce calculations; the model
explains them under claim verification.

Run `cd agents && bun evals/product-audit.ts` to reproduce current structural
counterexamples. Scripted models/sources diagnose architecture constraints only.
They do not measure real model quality or customer task completion. Future live
runs must record model/version, prompt/tools, data releases, budget, outcomes and
failures on the same cases. Keep a held-out set and report all eligible cases.

## 6. Attack correctness, usefulness and self-deception

Use separate review perspectives: skeptical finance user, skeptical data consumer,
assessor/consultant, lay portfolio user, competing-tool advocate, evidence/numerical
reviewer, operator and portfolio-project reviewer. These are review roles, not
fabricated interviews or a claim that multiple independent reviewers participated.

Cases must include: no data; partial coverage; ambiguous instrument; ticker changes;
corporate actions; unknown currency/quotation basis; revised macro history;
future information leakage; unequal frequencies; stale-but-newly-built snapshot;
duplicate news; manager disagreement; unknown publication date; opinion versus
measurement; false causality; missing holdings; uncited synthesis; prompt injection;
rights/access restrictions; excessive cost; changing source versions; and handoff
that silently drops evidence.

Ask “what would make this conclusion wrong?”, “is the citation actually supportive?”,
“can the user finish without the builder helping?”, and “would no agent be better?”
Let a task fail or return an explicit eligibility report. Do not count a safe
refusal as a completed requested analysis; measure correct abstention separately.

## 7. Apply outcome gates before promoting a roadmap item

These are proposed pilot thresholds, not established benchmarks or achieved results:

- Zero high-severity numerical, unsupported-claim, access or information-cutoff
  violations in the evaluated sample. This is not a guarantee of zero production
  risk. One such failure blocks the affected workflow; an average cannot hide it.
- Every reported numeric result must reproduce from its recorded inputs/method.
  Every material factual claim must have a supporting, inspectable source.
- At least 80% of eligible held-out tasks produce an acceptable finished artifact
  without builder assistance. Unavailable tasks must explicitly identify the gap.
- Aim for at least 30% lower median active work on paired tasks completed correctly
  by both methods. Also report failures/abandonments and all task counts; do not
  exclude unsuccessful OFL tasks to make the time comparison look better.
- Test comprehension separately for lay users: they can identify the main finding,
  uncertainty, source opinion and missing data without assistance.
- A data worker can reproduce the output on a second environment from its manifest,
  code/configuration and permitted inputs; no undocumented local state is required.
- Observe return use on another natural task/cycle; compliments and one demo do not
  demonstrate retention. Agree a pilot cost/latency budget before live model runs.

Have a second qualified reviewer grade financial/analytical artifacts when one is
available. Record disagreement; do not invent independence when only the builder
has assessed the result. Small pilot results guide decisions, not population claims.

## 8. Decide, learn and demonstrate

At each review: continue, narrow, change cohort, change workflow, or stop a weak
feature. Every decision records evidence, counterevidence, remaining uncertainty,
owner, next experiment and a reversal condition. Revisit persona weights as actual
usage replaces assumptions. Replace the inherited 20-item list with outcome-linked
priorities; it remains an ideas inventory until a task justifies each item.

Primary metrics: verified useful artifacts per returning user, eligible task
completion, reproduction success, active work saved, comprehension, coverage
failures, correction burden, cost and latency. Revenue/savings claims require
actual evidence; do not monetize hypothetical time saved as if measured.

Secondary engineering-portfolio track: one honest, reproducible five-minute case,
a public-safe dataset/fixture, architecture decisions and rejected options, failure
and recovery evidence, resource/cost measurements, evaluation results and known
limitations. A reviewer should see why the engineering helps a real task. More
infrastructure is not automatically a better portfolio project.

Proposed sequence: first complete the factual/structural audit (done in this pass),
then run workflow interviews and paired baselines, then a bounded vertical slice,
then held-out trials and repeat-use observation. Calendar estimates depend on
participant access and data clearance; no interviews or future work are scheduled
or claimed completed by this document.
