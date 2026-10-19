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

## Investigate a chosen source set

Finance researchers and data workers can now select one to four passages in
Pesquisa and choose **Investigar seleção no assistente**. The question is prefilled
and remains editable before execution. This is an author-curated research path;
it does not claim automatic relevance or close the original failed T01.

The agent receives a strict `OFL_SOURCE_REQUEST:` packet with `version: 1`,
`question` and distinct `passage_ids`. IDs authorize requested reads, not source
text or review judgments. The normal authenticated passage API and fresh canonical
verification still apply. No automatic search replaces these sources. An invalid
packet or failed read stops the flow; an inactive/changed passage is excluded with
an explicit partial-coverage notice. The brief records requested and revalidated
passage counts. Four passages do not imply four independent documents.

Manual selection rereads the chosen passages without an additional publication
filter. Date fields are disabled in this mode; describe the intended horizon in
the question and inspect the catalogued dates in the response. Unknown publication
stays unknown. Optional assisted classification remains a strict tentative relation,
not free substantive synthesis. Source-only mode performs no model call.

The panel checks the authenticated agent catalog capability before sending a
selected-source turn. An old/incompatible agent fails visibly, preventing a mixed
rollout from silently turning manual selection into automatic search. Reopening a
conversation restores its last chosen set; opening another conversation clears that
set. The dossier handoff keeps the actual question and source IDs.

Development acceptance covers exact chosen reads, malformed/duplicate/path/excess
IDs, failed access, source-only privacy, durable ownership, UI packet round trips,
reopened conversations, old-agent refusal and 390px layout. Browser acceptance uses
synthetic responses; it is not a live VPS trial or a relevance/support assessment.
The original T01 outcome remains failed until its substantive task is satisfied.
