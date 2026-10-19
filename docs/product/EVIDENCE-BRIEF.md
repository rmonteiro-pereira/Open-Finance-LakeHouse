# Evidence brief and assisted comparison

The research agent now produces a source-backed brief with the user's question,
source titles, cataloged publisher/manager, publication intervals or explicit
unknown dates, source type, content limits, exact quotes, text hashes, comparison
boundaries, unresolved counterevidence and a next step into the dossier.

Source type is explicit: a news report is not a reviewed financial observation;
a manager's projection remains attributed opinion. A month in a letter's title
does not establish its publication date. A headline-only record does not imply
that the article body was inspected. Countries, indicators and horizons need
review before interpreting a disagreement. Source verification establishes
literal identity, not relevance or entailment.

Before composing the brief the agent re-reads the passage and uses its current
metadata; inactive or changed text cannot establish a new brief. Failed tool
reads stop instead of repeating until the turn budget is exhausted. Publication
and collection fields are available in the authenticated passage API and tool.

## Optional model role

Two CPU trials of free qualitative prose on real source captures produced an
unsupported comparison and misattribution despite shorter instructions. They are
retained as failed experiments. The published path instead asks the existing
model for only `relation` and source indices: `context`, `contrast` or
`insufficient`. The strict schema rejects every extra field, including model
prose. A proposed contrast requires two distinct source items; references must
exist, be unique and be in range. The displayed explanation comes from code.
The relation is still a tentative machine judgment requiring source review.

The classification receives the original quote windows and publication metadata,
so it can inspect actual indicators and horizons. Its output schema has no field
for numbers or generated prose. Original quotes, source text, dates and financial
values remain preserved in the brief and evidence. This step cannot perform
quantitative synthesis. Source-only mode performs no model call. A model
failure leaves the source-backed brief available with the failed classification
explicit. The 45-second role deadline and VPS-only deployment remain unchanged;
the request output ceiling is now 48 tokens. No model upgrade or desktop service
is introduced. Approved Inkwell 0.1.1 interfaces are used unchanged.

## Evaluation boundary

Test malformed output, injected prose, invented and duplicate references, two
passages from one document masquerading as a counterpoint, unknown publication,
headline-only scope, changed attribution, inactive revisions and read failure.
Check actual source quotes and metadata after deployment, including source-only
and comparison modes, saved-conversation reopening and mobile presentation.

A complete T01 analytical brief still needs independently reviewed relevance,
appropriate sources and supported substantive synthesis. This implementation
improves the inspectable brief and its failure behavior; it does not turn the
previous T01 failure into a pass or establish independent user outcomes.

Production stage acceptance, the still-failed substantive T01 and retained native
experiments are recorded in `OUTCOME-TRIALS.md` and
`evidence/evidence-brief-live-acceptance.json`.
