# Research implementation decisions

The architecture discussion selected a private research app, all supplied MUB3
item types, COTAHIST from 1986, OCR/table review, and admin actions. Actual source
files and a judged search corpus have not been supplied. Implementation keeps
those external dependencies explicit.

## Separate quote lane

The old security fact deduplicates on symbol/date and is unsuitable for all-market
COTAHIST. The new fact retains market/contract identity and exact decimal values.
It uses bounded Arrow staging, DuckDB validation and delta-rs date replacement
rather than adding another full-history Spark MERGE to the nightly single-node
pipeline. Existing source tasks and equity/futures marts remain operational while
the new fact is evaluated. A compatibility cutover is withheld until real files can
be compared; the new fact is usable independently through the private app/API.

## PostgreSQL is the application authority

Reviews, metadata revisions, job leases, import authority and serving pointers are
transactional PostgreSQL records. Immutable originals and extraction artifacts
live outside PostgreSQL. Delta stores quote/history releases. Search content is
revision-linked and rebuildable. Publication records an exact Delta version; an
uncommitted Delta write does not become visible through the API.

## Retrieval backend remains a measured decision

Keyword plus exact pgvector is the implemented baseline because it can join the
current passage revision and catalog filters directly. This is not a conclusion
that it outperforms Qdrant, Neo4j, FalkorDB or ArcadeDB. The local Qdrant pilot
checks retrieval parity with identical embeddings and judgments. Its in-process
timings cannot decide server performance. Use representative text, filtered ANN
tests and concurrency/resource measurements before enabling ANN or selecting a
different serving engine. Graph candidates require explicit relationship-based
questions and curated entities; no entity graph is fabricated from OCR alone.

## Private application boundary

The existing snapshot application and the new research application have separate
Next.js route trees. Research API routes are absent from the public build, not
merely hidden in navigation. The private app requires gateway identity and a shared
server-side secret; state changes additionally require an exact origin. Production
network isolation is part of that authentication boundary.

## Archive fidelity before inferred financial meaning

All supplied item types are cataloged. OCR can be searched without numeric
approval. Candidates retain source regions/cell boxes; approval requires resolved
dates, units, market and column mapping. Approved observations are a separate fact,
not replacements for COTAHIST. Audio/video transcription and unattended source-site
acquisition remain outside this delivery.
