# Dataset delivery through the research agent

The agent's descriptive dataset journey uses the existing authenticated dataset
service. A request, including one with dates, first reads the real macro catalog.
The user confirms series IDs, one snapshot, dates, transformation and date
alignment in the conversation. Separate catalog queries are combined for this
choice; similarly named series are never selected silently.

The confirmation submits a complete `OFL_DATASET_REQUEST:` recipe. The guided
reply validates its bounded schema, re-reads the pinned catalog, rejects missing
or unpopulated series, then calls `prepare_dataset`. Only this turn's successful
tool results can establish preparation. Source failures stop the workflow rather
than retrying until the turn budget expires. No model generation is needed.

The result contains the canonical specification, preview, row count and CSV
SHA-256. A conversation result card revalidates that hash before downloading the
existing authenticated ZIP. The ZIP contains source inputs, CSV, manifest,
receipt, README and independent standard-library Python replay. Reopening a
saved conversation preserves its recipe; downloading still rechecks current
access and source availability. A prepared dataset is not a downloaded package.

The inline chooser applies one operation to all selected series. Difference and
percent change use lag one; rolling mean uses three observations. The general
Preparing data screen and explicit recipes support other bounded lags/windows
and per-series operations. The chooser does not infer these choices from prose.
Dates in a prose request are not automatically accepted as a confirmed recipe.
Nulls stay null; date alignment does not resample frequencies. Windows count
source observations inside the selected interval, without hidden warm-up.
Observation dates do not establish historical information availability.

Local verification: agent typecheck, 32 agent tests including PostgreSQL restart
and ownership, and the production Next dashboard build. Synthetic agent tests
include dated clarification, real tool chaining, malformed and reversed recipes,
missing series, backend refusal and mismatching specification. Production
acceptance is recorded separately after deployment; this does not validate the
analytical-brief journey or independent user comprehension.
