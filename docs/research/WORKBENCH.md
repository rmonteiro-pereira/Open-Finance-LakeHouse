# Workbench foundation

Product direction: [SHAPE.md](../product/SHAPE.md). Finance and data work are both
primary. This first slice connects existing assets to research and data journeys;
it does not yet implement portfolio holdings analysis or evidence-bound synthesis.

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
Portfolio guidance currently helps frame a research question; holdings are not
collected or analyzed in this slice.

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
