# Workbench foundation

Product direction: [SHAPE.md](../product/SHAPE.md). Finance and data work are both
primary. This first slice connects existing assets to research and data journeys;
it also implements owned research revisions, descriptive dataset preparation,
valued long-only portfolios/scenarios and bounded grounded agent reads.

## Operation

Use the dedicated research database and private storage settings described in
[RESEARCH.md](../RESEARCH.md). Apply migrations before starting the new API/worker:

```sh
ofl research migrate
ofl research sync --kind macro
ofl research sync --kind news --sync-limit 20
ofl research sync --kind letters --sync-limit 3
ofl research worker --drain
```

Sync commands enqueue jobs. A continuously running worker processes them. Repeat
bounded jobs to advance through unchanged-object checkpoints; narrow document
sources with `--collection SOURCE_ID`. News limits count batch objects, letters
limits count stored originals. Inspect jobs for failures; do not infer corpus
coverage from a completed small batch. PDFs enqueue the existing extraction/OCR
pipeline, whose completion is separate from registration.

A letter whose listing link text is a generic button ("Mais informações", "Leia aqui",
"PDF") is titled from its file name instead (`ofl/documents/collectors.py`,
`link_title`). A title that names a month without its year ("Jun", "Junho", "Carta
Mensal Julho") gets the year from the first of these sources that states it
(`ofl/documents/periods.py`):

1. the file name: a month and year ("Carta_do_Gestor-Junho_2023.pdf", "Carta Mensal
   2025_04.pdf"), or a year alone ("Carta-1Semestre_2021.pdf") for the title's month;
   a file name that mentions another month or another year gives nothing;
2. the year heading the link sat under on the listing page, which the collector records
   as `listing_year` in the manifest, only on pages sectioned by year;
3. the upload date, when it falls in the month the title names;
4. the header date of the letter's first extracted page ("Junho/2023"), once the
   original has been processed: the first month and year in its first 300 characters,
   which must be the title's month (a later mention is usually a comparison).

Each source must agree with the month the title names; a source that gives two years,
or a year after the current one, gives nothing. When no source states the year the
title stays as it is: a year is never guessed. "Jun" becomes "Junho 2023", "Carta
Mensal Julho" becomes "Carta Mensal Julho 2025", and a title with no month ("COVID")
dated by its file name becomes "COVID - Março 2020". The letter's date is its upload
date when the manifest has one; otherwise the derived month dates it (month precision).

A manager that runs several funds often labels each fund's letter with the month alone,
so two letters share a title ("Dezembro 2024"). When the letters of one collection name
more than one fund, each letter's title starts with its fund ("Alfa FIA - Dezembro
2024"), unless the title already names it (`ofl/documents/funds.py`). The fund comes from
the first of these sources that names one:

1. the link text on the listing page ("Carta Alfa Macro FIA - Junho");
2. the file name ("7G_Alfa_FIA-Carta_do_Gestor-Dezembro_2024.pdf" gives "Alfa FIA");
3. a `fund` field in the manifest row, for a collector that knows it (none records it
   yet);
4. the header of the letter's first extracted page (its first 300 characters), read only
   for letters no other source names a fund for, once the original has been processed.

Every source follows the same rules, the manifest field included. A fund name must carry
a fund-type code in capitals (FIA, FIM, FIC, FIF, FII, FIDC, ...) and at least one other
word; after the code only qualifiers such as "Ações", "NP" or "Master" belong to it. The
name must be set apart by a separator, a document word ("Carta"), a month or a number:
a name inside running text ("um bom FIM de ano") or longer than six words before its code
gives nothing. A code with qualifiers only ("FIM CP") does not say which fund it is, and
outside a file name a lone "FIM" may be the word "fim" ("bom FIM de ano"), so neither
counts; next to another code ("FIC FIM") it does. A link text or manifest value that
would name a fund only if its lone "FIM" were the code ends the search: the letter gets
no fund, rather than one from a later source that may be a different fund.
The manager's whole name is dropped from its front ("7G Alfa FIA" becomes "Alfa FIA"). A
source that names two different funds gives nothing, except two names joined with "+"
("Beta Hedge FIM + XY FIA"), which is one letter for both funds. A letter no source names
a fund for keeps its title, so two letters can still share one; a fund is never guessed.

The letters sync derives the title and date from the manifest row each time (and, for
the fund, from the other letters of the same collection), and both are part of the
letter's sync signature. Letters whose title or date changes are
therefore synced again, and only those: the item title, its heading vector
(`item_headings`, kept by a trigger) and its metadata passage are rewritten, and the
original is not processed again. Besides the manifest, the derivation reads the
letter's first extracted page and the current year (later years are rejected as errors),
so a letter can be retitled without a manifest change: a new letter dated only by its
first page gets its year on the first sync after its original is processed. Letters collected before
listing years were recorded get them from the next `ofl letters collect`, which
updates the manifest without downloading the letters again. Body passages keep a hash
of the title for jurisdiction tags, so run the jurisdiction backfill afterwards:

```sh
ofl letters collect --manager SOURCE_ID
ofl research sync --kind letters --collection SOURCE_ID --sync-limit 20
ofl research tag-jurisdiction --limit 1000
```

The bridge reads existing S3 objects only; it does not crawl source URLs. Configure
`OFL_RESEARCH_LIBRARY_BUCKET` (default `documents`) and
`OFL_RESEARCH_SNAPSHOT_BUCKET` (default `ofl-public`), using the existing core S3
connection settings. Object, metadata and original-size bounds are configured by
`OFL_RESEARCH_BRIDGE_OBJECT_LIMIT`, `OFL_RESEARCH_BRIDGE_METADATA_BYTES` and
`OFL_RESEARCH_BRIDGE_ASSET_BYTES`. Ship `sources/news.yml` with the runtime so
recorded source policy and current configured policy can be enforced together.
Unknown news sources default to headline-only. Restricted source bodies are
excluded from both indexing and preserved research captures. Publication dates
retain their precision; missing dates remain unknown. Manager letters are
attributed opinion. No imported text becomes an approved numerical observation.

## Interfaces

The private dashboard starts with Início, Pesquisa and Dados. Search can be used
without a generation provider. Users inspect cited passages and original assets,
export references, discover macro series by name, preview observations and export
a version-pinned CSV plus its JSON manifest and Python verification recipe.
Carteiras accepts explicitly valued positions in one currency, computes weights
and linear hypothetical shocks, and preserves inputs/results as owned revisions.
It does not infer prices, FX conversions or suitability.

Authenticated REST endpoints under `/api/v1`:

- `GET /workbench`: indexed coverage and current macro snapshot.
- `POST /library/sync`: administrator-only, same-origin, idempotency-protected job.
- `GET /data/catalog`: open series, actual coverage, snapshot identifier.
- `GET /data/series/{id}`: bounded observations and scope.
- `GET /data/series/{id}/export`: CSV, explicit snapshot required.
- `GET /data/series/{id}/manifest`: CSV hash, source hash, scope and qualifications.
- `GET /passages/{id}`: cited text and provenance.

MCP and Inkwell use authenticated read-only tools over these same contracts.
The existing reader/reviewer/admin roles and per-user conversation ownership remain
in force. Policy downgrades hide retired unrestricted news passages and assets.

## Reproducibility and limits

A macro snapshot is served only after its declared checksum, schema, finite values,
unique series/date keys, open-series scope and actual coverage are validated.
Serving switches atomically after validation. Older versions remain addressable.
CSV and manifest exports use the executed query scope and snapshot even when the
user subsequently edits pending input fields. The manifest records the CSV hash.
Macro values preserve the source numeric representation; this does not turn
floating-point source data into exact financial decimals. Observation dates do
not establish what information was available historically.

Local automated checks exercise PostgreSQL transactions, synthetic object-store
fixtures, API authentication, source restrictions, immutable versions and export
hashes. They do not establish real-user research quality or production deployment.
Real-source validation, browser acceptance and deployment must be recorded
separately before being claimed.


## Research artifacts and analytics

`/api/v1/workspaces` lists/creates only the caller's work. Kinds: investigation,
dataset, portfolio, watchlist, check. POST `/workspaces/{id}` requires the expected
revision; stale writes return 409. Revisions are append-only. GET endpoints expose
history, revision-pinned JSON/Markdown exports and explicit differences. Source
observations must quote actual captured passages; interpretations remain hypotheses.
Saved source captures honor subsequent news restrictions when read/exported, with
redactions distinguished from the original stored checksum.

POST `/datasets/prepare` and `/datasets/export` accept typed snapshot-pinned specs;
GET `/datasets/preview` exposes the read-only computation to MCP. Up to eight series,
outer/inner observation-date alignment, level/difference/percent-change/index-100/
rolling-mean, explicit lag/window, no filling. CSV hashes and full specifications
appear in manifests. Saved recipes reopen against their original snapshot.

GET `/quotes/compare` rejects ambiguous identities/bases and calculates exact
observed endpoint price changes. GET `/releases/diff` returns bounded record-level
differences. Neither implies total returns or continuity through corporate actions.

Saved checks rerun with POST `/workspaces/{id}/run`, append results and compare
content hashes. POST/GET `/workspaces/{id}/schedule` controls periodic checks from
15 minutes to one week. The worker leases jobs, avoids concurrent queued duplicates
and disables revoked owners. Results stay in-app; no external messages are sent.
Watchlists require ISIN, market, currency and distribution. Job events record
queued/running/terminal transitions prospectively, without inventing old history.

`/feedback` supports categorized reports; reviewer/admin triage appends history
and can mark evaluation candidates. Candidates are not passing evaluations.
Search results disclose keyword/vector ranks, raw scores and RRF contributions;
these describe ordering, not truth probabilities.


## Guided source agents

Enable `OFL_AGENT_GUIDED_DOCUMENTS=1` on the private Inkwell service. Document
research uses mandatory bounded source searches and context reads, independent
canonical verification, then an optional qualitative interpretation. This is an
explicit workflow, not a claim that model inference performed the collection.
Source-only requests skip generation. Interpretations have a separate 45-second
budget and may fail without replacing real source evidence with invented text.
Mixed questions can discover macro series and read exact requested series IDs from
the returned catalog; ambiguous definitions remain a user choice. Numeric tables
keep their own snapshot, units and qualifications. The GUI can create a dossier
from conversation references and executed tool scopes directly.

Remote CPU embedding indexing submits one complete passage per request and saves
each result before proceeding. This bounds inference batches without truncating
text, weakening model-digest checks or losing completed vectors on a retry.

Scoped semantic-index coverage, fair worker pools and judged retrieval evaluation
are documented in [RETRIEVAL-EVALUATION.md](RETRIEVAL-EVALUATION.md). Coverage is
reported independently of ranking relevance and source completeness.
