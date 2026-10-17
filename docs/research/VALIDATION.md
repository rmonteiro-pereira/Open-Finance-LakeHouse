# Research validation — 2026-10-16

Validation used a disposable local PostgreSQL 17/pgvector instance. No production
database, source-site downloader, or real archival corpus was used.

Verified:

- Final host suite: **194 passed**, with two OCR-tool tests skipped on the host.
  Those same two tests passed separately in the OCR container, covering both PNG
  and scanned PDF input. No production data was used in these checks.

- Existing offline suite plus isolated research integration tests pass. Tests cover
  all documented COTAHIST markets, exclusions, identities, decimals, replay,
  daily/annual precedence, explicit corrections, whole-date removal, conflicting
  duplicates, interrupted Delta publication, and expired job leases.
- Catalog revisions remove superseded text from retrieval. Hybrid/vector queries
  honor catalog filters and inactive revisions. Exact pgvector/local-Qdrant parity
  passes on fixed vectors with explicitly synthetic judgments.
- API tests cover gateway authentication, reader/admin separation, origin checks,
  idempotency conflicts, upload offsets/checksums, exact decimal JSON, review
  concurrency and observation supersession.
- Research API/OCR container builds from the locked dependencies. Poppler,
  Portuguese/English Tesseract and OpenCV processed a synthetic scanned PDF,
  extracted its text, and reconstructed a three-column ruled table.
- BAAI/bge-m3 at revision `5617a9f61b028005a4858fdac845db406aefb181` produced
  1,024-dimensional embeddings and retrieved the synthetic OCR passage through a
  Portuguese paraphrase. This confirms wiring, not corpus-level retrieval quality.
- Both Next.js apps build. The public app's route output contains only `/` and
  `/_not-found`; private proxy routes occur only in the separate research build.
- Chromium smoke checks exercised quote queries, archive search, document/page
  evidence, table-review loading, and operations, with no browser errors. Desktop
  and mobile screenshots were inspected locally. Gateway headers were simulated
  only against the loopback test deployment.

Not established:

- Real COTAHIST coverage from 1986, MUB3 inventory completeness, bulletin layout
  diversity, legacy equity compatibility, or production freshness improvements.
- Accuracy of OCR financial values without review.
- A production retrieval-backend winner, filtered ANN recall, or capacity at the
  actual passage count. Local in-process Qdrant timings are explicitly qualified.
- Production authentication/network isolation, backup recovery, or deployment.
  Kubernetes connectivity timed out from the implementation environment. No
  administrator identity was guessed and no production manifests were applied.

Repeatable commands and environment settings are documented in `docs/RESEARCH.md`.
The dedicated CI workflow runs the database and OCR tests and both web builds.
