# Research quality and task-outcome trials

## Operating boundary

The desktop GPU was used only for the completed backfill. It is not a permanent
backend, online fallback, or a prerequisite for using OFL. Production runs on the
VPS. Evaluate improvements within that budget; do not introduce a desktop routing
dependency or buy a provider without a separate decision.

## Source relevance before an analytical conclusion

Search ordering returns candidate documents. A nearest neighbor is not a finding
that the document addresses the question. No similarity cutoff is promoted from
the six authored development cases. Collect actual judgments first, including
paraphrases, irrelevant near-neighbors, absent topics, dates and opposing views.

In a dossier, read each selected passage and record relevant, possible
counterevidence, irrelevant or uncertain, with a rationale. The server attributes
the judgment to the authenticated owner and binds it to the exact question and
captured source metadata/text. A changed question or capture requires re-review.
These are author judgments, not independent ratings or verification of an
interpretation. Explicitly select citations for each finding; the interface no
longer attaches every dossier reference to every new finding automatically.

The immutable revision and Markdown/JSON exports preserve these reviews. Current
source restrictions redact linked review text as well as excerpts/findings.
Incomplete reviews do not block saving an unreviewed dossier, but a selected
verdict needs a rationale. Correct literal quotation still does not prove
relevance, causal interpretation, return or suitability.

## Agent rehearsal on the VPS

The interface defaults to collecting and checking documentary sources. Requesting
an interpretative draft is a separate experimental choice. This keeps a useful
source-to-dossier path available without counting a generation timeout as analysis.
Other tasks may still require the model for planning or clarification.

Run at most six development cases with private gateway credentials in the
environment. The output directory must not exist; it is created with mode 0700
and artifact files with mode 0600. Do not put private artifacts, source bodies,
identities or credentials into the public repository.

```sh
python tools/run_agent_trials.py docs/product/agent-trials.json \
  --url "$OFL_EVAL_AGENT_URL" --output /private/trials/new-run
```

Set `OFL_RESEARCH_GATEWAY_TOKEN`, `OFL_EVAL_IDENTITY` and `OFL_RESEARCH_ORIGIN`.
The runner records the actual agent configuration, HTTP failures, time and artifact
hashes. Every outcome initially remains unassessed, including HTTP 200 responses.
This is a builder-operated rehearsal, not a participant or held-out study.

## Manual assessment and paired participant study

For each actual trial, inspect the artifact against its required deliverable and
hard gate. Add an assessment to the private `assessments.json`, for example:

```json
[
  {
    "case_id": "T01",
    "condition": "ofl_agent",
    "artifact_sha256": "COPY THE RECORDED HASH AFTER INSPECTING THE ACTUAL FILE",
    "reviewer": "ACTUAL REVIEWER",
    "eligibility": "eligible",
    "outcome": "failed",
    "hard_gate": "pass",
    "claim_support": "pass"
  }
]
```

Eligibility is `eligible` or `unavailable`. Outcomes are `completed`, `failed` or
`correct_abstention`. Each gate is `pass`, `fail` or `not_assessed`. A claimed
completion/abstention without both review gates passing is a review-gate failure.
An unavailable task cannot be a completed task. Artifact hashes must match both
the recorded trial and the actual private file; runtime status is not a grade.

```sh
python tools/evaluate_product_trials.py docs/product/agent-trials.json \
  /private/trials/new-run/trials.json /private/trials/new-run/assessments.json \
  --artifacts /private/trials/new-run
```

All planned cases appear for OFL with agent, OFL without agent and the user's
baseline. Missing trials remain visibly unassessed; eligible assessed denominators,
review failures and correct abstentions are separate. Reviewer identity is
self-reported by this offline tool, not independently authenticated. Do not label
builder assessments independent validation.

Use WORKFLOW.md's planned eight-person discovery round: finance researchers, data
workers, assessores/consultores and lay market/portfolio users. No participants
have been recruited by these tools. For each actual participant, record role and
experience, a recent task, permitted inputs, agreed cutoff and the required
artifact before testing. Counterbalance the three conditions, retain unsuccessful
attempts, and record assistance, active work, elapsed time, source/correctness
review, comprehension and willingness to repeat the task. Lay users must explain
which statements are opinion and what remains unknown. Data workers must reproduce
the dataset from its manifest in another environment. Preserve reviewer disagreement.

Do not infer adoption, savings or a performance winner from a builder run. Promote
synthesis or a retrieval calibration only after independent task judgments support
it, including negative and ambiguous cases. These trials have been initiated;
participant outcomes and independent judgments remain uncollected.


## Baseline and bounded fixes

The initial production CPU rehearsal returned a source-only T01 turn in 52.729 s
with quotations outside the inflation topic and no requested brief; T02 reached
the client's 125 s timeout; T10 returned a qualified gap in 2.526 s. Builder
assessment records two failed eligible tasks and one correct abstention, with
baseline/no-agent conditions unassessed. See evidence/vps-agent-outcome-baseline.json.
These observations do not establish independent relevance or user performance.

The document guide now isolates the `sobre …` topic clause and conjuncts its
keywords instead of OR-matching analysis instructions. Explicit `q=` syntax is
preserved. Quote windows prioritize topical terms rather than the first generic
country mention. This can reduce lexical recall; it is not a calibrated semantic
abstention mechanism. Validate paraphrases and opposing views independently.

Dataset requests missing a period read the actual catalog for requested series
and ask for period, frequency/alignment and missing-value rules without generated
planning. Failed catalog access remains a failure to establish eligibility, not
an empty catalog. Fully dated requests retain the existing preparation route.
This completes a scoping/clarification step, not a dataset export. The documented
T02 rehearsal is explicitly a scoping variant of the larger dataset-delivery task.


## Deployed follow-up, not participant validation

Public #73 and the private agent were deployed with all seven services ready.
The same T02 preparation-scoping prompt returned actual catalog metadata and
missing-rule clarification in 0.922 s; it did not prepare/export a dataset. T10
returned a qualified gap in 1.181 s. A separate T21S source-collection stage
returned inspectable references in 3.821 s; it is not the full T21 dossier task.
T01 returned in 29.888 s but still failed the requested brief, with no valid
interpretation. A geographic ambiguity in the selected news headline remains a
review issue. These are single builder-operated development observations on a
changing live corpus, not controlled user-performance measurements.

The production application browser exercised source-collection mode, actual agent
to dossier handoff, source inspection/review controls, question invalidation,
explicit citation selection and mobile layout with zero JavaScript errors.
Access used the SSH loopback trusted application boundary with configured identity
and origin injected; Tailscale authentication was not retested. The dossier was
left unsaved in this browser probe. A separate owned production API probe saved,
exported, rejected a stale review and removed only its temporary dossier. Full
save/export browser acceptance also passed on disposable local PostgreSQL.

A larger Qwen3 1.7B was compared with the production 0.6B alias on the VPS CPU,
using the same captured source excerpts and request-local context of 2,048,
two threads, batch 64, 128 output tokens and a 45 s generation limit. Model load
was recorded separately. The 0.6B finished in 42.943 s without a mechanically
valid draft; 1.7B timed out at 45.062 s. Neither configuration is promoted.
The production baseline was reloaded with its original context. This establishes
neither independent claim support nor general model quality. See the sanitized
follow-up/model reports in evidence/; private raw artifacts remain on research
storage.

Reproduce the bounded model comparison against a private recorded source artifact:

```sh
python tools/compare_interpretation_models.py /private/trials/run/T21S-ofl_agent.json \
  --url "$OFL_EVAL_MODEL_URL" --baseline "$OFL_EVAL_BASELINE_MODEL" \
  --candidate qwen3:1.7b --output /private/trials/run/model-comparison.json
```

Download both models on the VPS first. The tool records model digests and excerpt
hashes, keeps raw output private, and reloads the baseline with the current OFL
production context/thread profile. It does not select a production model or add
a desktop dependency. Independent relevance judgments and actual participant
completion/comprehension/reuse studies remain uncollected.


### Agent dataset delivery follow-up — builder acceptance

The dated IPCA/Selic 2020–2025 request now remains guided. The browser confirms
actual catalog IDs, one snapshot, operation and date alignment, then the agent
re-reads that snapshot and calls preparation without generated planning. The
result has 1,536 rows and its CSV is byte-identical after isolated stdlib replay
on both the VPS and a separate temporary client. The package is downloadable
from a reopened, owned conversation, with hash revalidation before download.

The first narrow-screen test failed on recipe text/selector overflow. OFL #80
corrects the presentation; the final production rerun is recorded in
`evidence/agent-dataset-live-acceptance.json`. The previous failed and scoping-only
rehearsals above remain historical evidence. This builder-operated confirmation
flow does not establish participant success, autonomous interpretation of every
recipe from prose, or completion of the failed T01 analytical brief.


### Source-backed brief and assisted comparison — builder follow-up

The agent now composes a source-backed brief with current publication/attribution,
source type/content limits, exact quotes, comparison boundaries and explicit
unresolved counterevidence. Each quote and its current metadata are re-read before
composition. Unknown publication stays unknown even when a letter title includes
a month/year. Inactive or changed passages cannot establish a new brief. Failed
reads stop without exhausting the turn budget.

Two native CPU free-prose prototypes failed builder review (unsupported comparison
and misattribution). The optional published model step instead outputs only a
strict relation and source indices. Source text, numbers and publication metadata
remain available to that classifier; every extra output field, including prose,
is rejected. A contrast needs distinct source items and remains a tentative
machine judgment. The interface calls this assisted comparison. Guidance respects
intentional country/horizon comparisons in the user's requested scope.

The production rehearsal retained the original substantive T01 requirement:

| Case | Observed result | Elapsed, one observation | Builder outcome |
|---|---|---|---|
| B01 source-only brief stage | Two quotes, publishers and publication metadata match the current API; source types and limits explicit | 4.442 s | Completed the evidence-brief stage |
| T01 substantive comparison | Valid `insufficient` classifier result; the same inspected news/letter pair still has a country mismatch and unknown letter publication | 27.081 s | Failed the full analytical task; substantive claim support unassessed |
| T10 absent topic | Qualified gap in the requested indexed scope | 0.871 s | Correct abstention |

Four literal quote/hash and current publisher/publication checks passed across
B01 and T01. That verifies quotation and metadata identity, not relevance or
entailment. The same source pair and full source text as the earlier inspected
captures were observed; these runs are not a controlled benchmark or evidence of
quality lift. The failed T01 remains failed despite HTTP 200 and a useful brief.
Baselines, independent support/relevance judgments and participant outcomes remain
unassessed. Native prototypes, source captures, private trial artifacts, manual
assessments and the formal evaluator output are retained on the research PVC.
The sanitized deployment/browser receipt is
`evidence/evidence-brief-live-acceptance.json`.
