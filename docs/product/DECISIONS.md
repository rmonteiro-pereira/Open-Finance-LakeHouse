# Proposed product decisions after the first adversarial review

Date: 2026-10-18. These are evidence-informed proposals, not customer validation,
a commitment to build every feature, or a claim that a live agent is already ready.

| Decision | Evidence / uncertainty | Next discriminating experiment | Reverse if |
| --- | --- | --- | --- |
| Serve finance workers and data workers on a shared trusted data foundation | User direction; substantial macro/document assets; current tools disconnected | One evidence-backed brief and one reproducible dataset against the actual workaround | Shared infrastructure does not reduce work or the cohorts need incompatible workflows |
| Include assessores, consultores and lay investment-portfolio users explicitly | User direction; different explanation/verification/holdings needs | Separate client-preparation and lay comprehension tasks using the same evidence | A specific cohort gets no repeat value; change its workflow rather than pretending the analyst UI fits |
| Put macro, news and manager letters ahead of more exposed source-specialist bots | Live collection inventory; agent tool audit AE04; raw source quality remains unverified | Curate one topic/period, connect it end to end, compare with manual reading and OFL without agent | Added synthesis is not useful, dates/quality cannot support the task, or a simpler existing service wins |
| Replace the blanket prose suppression only with a verified reasoning/writing path | AE01–AE03 show lost clarification/knowledge and blocked context reads; AE05 protects exactness | Bounded multi-step retrieval + deterministic calculations + claim verification on held-out tasks | Claims/citations/numerical checks fail or the extra complexity adds no task value |
| Keep a direct data-worker path through files/API/notebooks/BI | Current snapshots/exports already have data value; agents do not automatically improve those tasks | Reproduce a versioned dataset on a second environment with/without agent | Reproduction needs hidden state or users still rebuild source integrations |
| Separate descriptive research from historical performance claims | Unknown publication dates; original-price basis; no complete availability-time history established | Produce an eligibility report and a reproducible research specification before any backtest | Required inputs cannot be obtained; keep the supported descriptive workflow and narrow the strategy claim |
| Treat the inherited 20 features as hypotheses | No measured adoption or task outcomes; implementation-first backlog | Map each item to persona, real task failure, source readiness and observable outcome | A feature cannot justify its cost or simpler existing capabilities finish the task |
| Keep engineering portfolio secondary but make it stronger through product evidence | User instruction; existing technical foundation is substantial | Five-minute public-safe case showing a real result, reproducibility and failure evidence | The demo optimizes stack complexity or synthetic success instead of a truthful user outcome |

After actual trials, append dated decisions with artifact IDs, disagreements,
measured task denominators, next owner/action and a reversal condition. Do not
silently rewrite an early hypothesis into a retrospectively certain decision.


2026-10-18 user direction: proceed through the whole approved shape sequentially,
including the inherited backlog as scoped in `../research/PRODUCT-BACKLOG.md`.
This authorizes implementation; demand and outcome hypotheses still require trials.

## 2026-10-18 — Desktop GPU is a one-time backfill resource

The desktop is not a permanent inference backend or online fallback. Keep serving,
search and agent execution on the VPS. Continue relevance assessment, source-to-
dossier workflow work and VPS-budgeted agent evaluation. Retain the completed
GPU backfill receipt without making desktop uptime a product requirement. See
OUTCOME-TRIALS.md for development rehearsals and the participant study protocol.
