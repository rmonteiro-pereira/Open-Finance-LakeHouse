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

## Live deployment verification (2026-10-17)

PR #60 was merged and the private services were deployed through the infrastructure
repository. Verified Tailscale identity receives the administrator role; forged
identity headers are overwritten, and cross-origin mutations are rejected. The
web server binds host loopback only. Desktop/mobile browser checks passed without
JavaScript errors.

A temporary database schema on the live shared Postgres passed scanned-PDF OCR,
ruled-cell extraction with provenance, the actual private Ollama BGE-M3 model,
Portuguese hybrid search, exact-decimal review approval, and Delta observation
publication. The temporary schema and its synthetic records were removed. CPU
thread limits are explicit because the live OCR check exposed oversubscription.

Real source checks found issues that synthetic fixtures do not show:

- The official 1986 annual file has contradictory forward-market records at source
  lines 140808 and 140809: VGO 2, 1986-10-10, market 030. Both originals are retained;
  the import fails closed and no quote pointer is published. Preflight now checks
  the complete staged file before any Delta write and reports the source lines.
- Current official files contain market code 021 with BDI 93, absent from the
  published layout's market table. The parser retains that exact observed pair as
  `unclassified_021`, separate from cash/fractional markets; no market meaning is
  inferred. Other unknown codes/pairs still fail closed.
- The 2026-10-16 daily file has 23,020 physical records but declares 23,018 in its
  trailer. It is correctly rejected rather than relaxing the count validation.

Source layout linked by B3 when checked:
https://www.b3.com.br/data/files/33/67/B9/50/D84057102C784E47AC094EA8/SeriesHistoricas_Layout.pdf

Original files used:
https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A1986.ZIP
https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D16102026.ZIP
https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D05012026.ZIP

The full 1986-onward backfill and real MUB3/bulletin corpus ingestion are not
complete. Optional Airflow triggers remain disconnected; the dashboard disables
those controls and the API rejects them before enqueueing. The dedicated research
worker handles imports/OCR/search/publication independently of Airflow.
