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
