# Private market and archival research

This lane imports **supplied files**. It does not fetch B3/MUB3 pages. It is
independent of the existing 51-series daily pipeline and public snapshot exporter.
Existing futures ingestion and equity marts are unchanged.

## Run locally

```sh
uv sync --locked --extra dev --extra research --extra research-ocr --extra research-pilot
```

Provide `OFL_RESEARCH_DATABASE_URL` for a **dedicated PostgreSQL database with
pgvector**. Do not use Airflow's database. `ofl research migrate` creates the
extension and schema, so run that command with a migration role; use an application
role for the API/worker afterwards.

Every command, worker and the API retries **opening** a database connection while the
server cannot be reached, so a sync, a scheduled job or a worker that starts during a
short PostgreSQL restart waits for it instead of failing. Only opening is retried:
nothing has been sent yet, so a retry cannot repeat a write. A connection lost inside a
transaction still fails at once, and so does opening one after the budget below.
`OFL_RESEARCH_CONNECT_ATTEMPTS` (default 6) bounds the attempts and
`OFL_RESEARCH_CONNECT_RETRY_SECONDS` (default 5) is the first wait, which doubles
up to one minute. The defaults wait 5+10+20+40+60 s, about 2 min 15 s, plus up to
5 s per attempt for the connect timeout. That covers a PostgreSQL pod restart on the
VPS, about 90 s from start to ready, with room to spare. While the database is
down, API requests wait the same way; set `OFL_RESEARCH_CONNECT_ATTEMPTS=1` on the
API to fail them at once instead.

```sh
uv run ofl research migrate
uv run ofl b3 import /path/to/COTAHIST_A2024.ZIP
uv run ofl archive import /path/to/catalog-package.zip --source mub3
uv run ofl archive import /path/to/bulletin.pdf --source b3_bulletin
uv run ofl research worker --drain
uv run ofl research coverage
```

`ofl b3 import ... --dry-run` validates COTAHIST without registering or publishing
it. Archive dry-run previews registration only; it does not claim to validate a
catalog's assets. The API runs with `ofl research serve`; a continuous worker runs
with `ofl research worker`. `docker/compose.research.yml` supplies a local database,
migration job, API and worker, with ports bound to loopback.

OCR requires `pdftoppm`, `tesseract`, Portuguese/English language data, and the
`research-ocr` extra. `docker/Dockerfile.research` contains those dependencies and
installs from `uv.lock`. A failed OCR job preserves its original and completed pages.

## Storage and identity

`OFL_RESEARCH_ROOT` is a persistent **private** working volume. Its defaults live
under ignored `data/research`. Do not share it with the public snapshot bucket.

| Prefix | Contents |
|---|---|
| `originals/` | SHA-256-addressed immutable supplied files |
| `staging/` | Bounded parsed Parquet, per import/parser version |
| `silver/fact_b3_quote/` | Delta quote history, partitioned by year |
| `silver/historical/` | Immutable releases of approved observations |
| `pages/` | Page previews, OCR text, word boxes and completion checkpoints |
| `exports/` | Release-pinned private Parquet exports |
| `orphaned/` | Interrupted first publications retained for diagnosis |

Set `OFL_RESEARCH_OBJECTS_BUCKET` to mirror originals to the existing S3-compatible
store under `research/originals/`. Connection settings are the existing `MINIO_*`
settings. Uploads/downloads stream; a cache miss checks the restored original's
checksum. Derived files and Delta releases still require the private persistent
volume. Neither originals nor derived files have an automatic deletion policy.
Back up that volume and the application database together.

Processing refuses new writes that would cross a 25% filesystem free-space
reserve or `OFL_RESEARCH_MINIMUM_FREE_BYTES`, whichever is larger. This is a
filesystem guard, not a guarantee about the remote S3 bucket's remaining capacity.

## COTAHIST contract

The parser follows B3's 245-byte revision-02 layout and preserves **all fields**.
Prices and turnover retain scaled integers and exact decimal amounts. Original
currency, quote factor, security specification, ISIN/internal identifier,
distribution, forward term, expiry and correction indicator are retained.

Included markets: `010,012,013,017,020,030,070,080`. Futures `050/060` and BDI
`70/71` are excluded. Conflicting futures classifications or unknown market codes
fail validation. There is no ticker watchlist or BDI-02-only filter.

Quote identity includes date, symbol, market, BDI, specification, ISIN/internal
identifier, distribution, forward term, expiry, correction indicator and currency.
Exact duplicate records collapse; different values for the same identity within
one authoritative input fail. Ticker equality does not imply instrument continuity.

Files must retain official `COTAHIST_Ayyyy`, `COTAHIST_Mmmyyyy`, or
`COTAHIST_Dddmmyyyy` names with ZIP/TXT extensions. The fixed-width header identifies
the year; it cannot encode the full daily filename. Filename/header coverage,
record sizes, trailer identity/count and individual values are checked.

Daily files outrank monthly files, which outrank annual files; source generation
date orders revisions within a frequency. Equal-priority conflicting inputs need
an explicit supersession:

```sh
uv run ofl b3 reprocess NEW_IMPORT_ID --supersedes PREVIOUS_IMPORT_ID
```

A complete-period correction can remove records or an entire date. Publication
selects one authoritative input per date, writes one date per transaction by
default, and switches a PostgreSQL serving pointer only after the complete import
succeeds. Readers pin the recorded Delta version. A later worker restores that
version after an interrupted publication before applying further changes.

`OFL_RESEARCH_BATCH_ROWS` defaults to 50,000; publication defaults to one date at
a time (`OFL_RESEARCH_PUBLICATION_DAYS`). DuckDB staging joins spill with a 512 MB
query memory budget. This bounds parsing and date scope, not arbitrary output size:
measure the largest supplied options day before increasing worker concurrency.

The existing equity fact is deliberately **not migrated yet**. The new private
quote fact is independently queryable. Replacing the old 15-ticker projection
requires comparison against real supplied inputs; no synthetic fixture can certify
that cutover. The legacy network task is consequently not disabled by this change.

## MUB3 catalogs and bulletins

A MUB3 ZIP package contains `catalog.jsonl` at its root and referenced assets:

```json
{"source_id":"catalog-123","title":"Historical publication","item_type":"document","collection":"Collection name","description":"Catalog description","date_start":"1950-01-01","date_end":"1950-12-31","date_text":"1950","date_precision":"year","subjects":["exchange history"],"creators":["Institution"],"url":"https://example.org/record/123","rights":"Supplied rights statement","assets":[{"path":"files/123.pdf"}]}
```

Standalone JSONL/JSON catalogs are supported without asset paths. CSV catalogs
require `--metadata` pointing to JSON with `columns` mapping canonical fields to
CSV headers. Missing source identifiers receive import-record identities; later
matching must be explicit. Missing dates remain missing. The original metadata is
retained. Photographs, objects, audio/video and metadata-only records are valid.
Audio/video is archived and cataloged, not transcribed.

Bulletin PDF/image/CSV files can be imported directly. Bulletin ZIPs may contain
ordinary assets without a catalog. Officially named COTAHIST members are routed to
the quote importer. Other CSV tables enter review rather than silently becoming
market facts. Additional source-specific typed adapters require real layout fixtures.

Native PDF text is extracted page by page. Pages with insufficient text use OCR.
Ruled scans produce candidate tables through line detection; native PDF tables
retain cell boxes. Other tables can be entered/corrected manually against the page.
OCR text is searchable before any numeric table is approved.

Review mappings explicitly specify date, subject, market, metric, value, unit and
optional currency, with decimal/thousands separators. Review explicitly selects
ISO or Brazilian day formats, month/year, or year. Monthly/yearly values retain
their interval and precision; the start date is not asserted to be an exact trading
date. Values are exact decimals. Revisions use optimistic concurrency; approval,
rejection and later correction preserve an audit trail. Approved observations retain
the source page, table region, cell coordinates where available, and approval revision.
They never overwrite COTAHIST. Table reconstruction is review-assisted, not an
assertion that OCR is numerically reliable.

## Private API and dashboard

The existing public app remains `web/app`. The separate app in `web/research`
reuses the web dependency installation but has its own route tree and build:

```sh
cd web
bun install --frozen-lockfile
bun run research:build
bun run research:start
```

Configure these settings privately:

| Setting | Purpose |
|---|---|
| `OFL_RESEARCH_API_URL` | Server-side URL used by the private Next.js proxy |
| `OFL_RESEARCH_GATEWAY_TOKEN` | Shared Next/API secret; empty fails closed |
| `OFL_RESEARCH_ROLES` | JSON mapping verified identity strings to reader/reviewer/admin |
| `OFL_RESEARCH_ORIGIN` | Exact browser origin; required for mutations |
| `OFL_RESEARCH_DATABASE_URL` | Dedicated application database |
| `OFL_RESEARCH_ROOT` | Persistent working volume shared by API/worker |
| `OFL_RESEARCH_CONNECT_ATTEMPTS` | Attempts to open a database connection (default 6; 1 turns retrying off) |
| `OFL_RESEARCH_CONNECT_RETRY_SECONDS` | First wait between attempts, doubling up to 60 s (default 5) |

The private Next.js server must be reachable **only through a trusted gateway**
that supplies/overwrites `Tailscale-User-Login`. The proxy strips client API identity
headers and injects the shared secret plus that verified identity. The backend is
internal only. Headers alone are not authentication if either service can be
reached directly by untrusted callers. Bind loopback behind Tailscale Serve or use
equivalent network isolation. Never set a client-controlled fallback identity.

Reader: browse/search/export. Reviewer: additionally edit/approve table candidates.
Admin: additionally upload, retry and run allowlisted actions. POST/PUT require
the exact configured Origin. Job creation and upload registration require an
Idempotency-Key; review saves use expected revision. Browser uploads hash in chunks
and resume from the committed offset. SHA-256 is verified before registration.

API routes are under `/api/v1`: `me`, `coverage`, `quotes`, `observations`, `items`,
`search`, `assets`, `candidates`, `imports`, `jobs`, `uploads`, and `exports`.
Monetary decimals are JSON strings. Quote page size is at most 1,000; exports cap at
one million rows and require narrowing beyond that. No arbitrary SQL, shell
commands, source URLs or filesystem paths are accepted from the browser.

Optional Airflow actions require `OFL_RESEARCH_AIRFLOW_URL/TOKEN` and use a fixed
DAG allowlist. A trigger response is an orchestration acknowledgment, not pipeline
completion. The optional hourly DAG is disabled until
`OFL_RESEARCH_AIRFLOW_ENABLED=true`; create the `ofl_research` pool with one slot and
the private Secret/PVC first. Use a continuous worker **or** the hourly DAG.
PostgreSQL serializes workers even if both are started accidentally.

## Semantic retrieval and backend selection

Keyword search is available immediately. Semantic search is opt-in and requires
the `research-search` extra, a pinned model revision, and generated embeddings.
PyTorch is resolved from its CPU wheel index for this extra; GPU runtimes are not
installed on the small server.

```sh
uv sync --locked --extra research --extra research-search --extra research-pilot
# Set OFL_RESEARCH_EMBEDDING_REVISION to a model commit, not a floating branch.
uv run ofl research embed --limit 1000
uv run ofl research worker --drain
```

The baseline model is BAAI/bge-m3, 1,024 dimensions. Set model, revision and
dimensions together when evaluating alternatives. Long passages are rejected at
the tokenizer limit instead of silently truncated. Changed passages get new
identities; inactive revisions never participate in retrieval even if their vectors
are retained. Embeddings are private and model/version-scoped.

Passages are windows of at most 350 words that also stay within an estimated
2,046-token budget (the deployed model's 2,048-token context minus its two sequence
markers; `ofl/research/chunking.py` documents the estimate and its calibration).
The budget matters for text whose extraction glued words together: such a window
ends early and the next one repeats a proportional overlap. Text whose windows all
fit is cut exactly as before and keeps its passage identities; text the budget
re-cuts gets a derived passage revision, so its new passages get new identities,
jurisdiction tags in document context and embeddings, while the superseded ones
and their rejections stop counting. Existing passages are brought in line with:

```sh
ofl research rechunk --dry-run   # chains to re-cut, passages superseded and new
ofl research rechunk             # one transaction per chain; rerunning finds nothing
ofl research embed --if-idle     # the embedding lane embeds the new passages
```

Original page text is not stored, so a chain is rebuilt from its passages' chained
overlaps (as in the jurisdiction backfill); a chain that does not rebuild exactly is
counted as `unchained` and left as it is.

Every transaction that writes an item's passages (catalog and archive ingestion,
news and letter sync, page processing, the jurisdiction backfill, rechunk) first
takes the same item-level advisory lock and reads the passage state it depends on
only after that; import and asset locks always come before it. Rechunk re-reads the
item's active passages under that lock and replaces a chain only if they are still
exactly the chain it planned; otherwise it counts the chain as `changed` and leaves
it to the writer that superseded it, so a concurrent re-ingestion or news policy
downgrade is never undone. The backfill likewise re-reads and tags each item in its
own transaction under the lock, so passages superseded meanwhile are no longer
pending. Bulk deactivations lock their rows in id order and new passages are written
in id order. Rechunk and the backfill hold one item lock at a time and touch only
that item's passages; the remaining deadlock risk is between writers that take
several item locks in one transaction in no fixed order (catalog imports, library
sync), which PostgreSQL detects by aborting one of them, whose job is then retried.

Saved investigations keep working across re-ingestion and re-chunking: when an
existing investigation is edited, a reference its previous revision already captured
may point to a superseded passage and keeps its stored text and reviews, unless the
current news policy restricts it. A new reference still needs an active passage.

A single whitespace-free run over the budget still forms its own window and stays
rejected by the embedding step rather than being cut inside a word.

The default research image omits model runtimes. Build with
`--build-arg INCLUDE_EMBEDDINGS=1` when enabling local inference, and persist
`HF_HOME` on private storage to avoid downloading weights on each restart. Measure
model memory separately from the default OCR worker's 2 GiB limit before deploying
it on the VPS; embedding generation can run from a workstation connected to the
private database. Query-time semantic search also requires an available model runtime.

Hybrid retrieval merges PostgreSQL keyword and vector rankings with reciprocal
rank fusion and groups hits by item. Exact vector retrieval is the default, with
source/type/collection/date filters applied to the eligible SQL rows. Opt-in HNSW
requires `ofl research index` and `OFL_RESEARCH_APPROXIMATE_SEARCH=true`; iterative
scans reduce filtered-candidate starvation, but recall must still be measured.

The pilot accepts JSONL with query, embedding, relevant passage IDs, and optional
source/type/collection filters:

```sh
uv run ofl research pilot /path/to/judged-queries.jsonl --limit 10000
```

It compares identical stored embeddings in exact pgvector and temporary local
Qdrant, reporting recall@10/nDCG@10 and qualified timings. **Local in-process Qdrant
is a correctness baseline, not a fair server-performance benchmark.** A final
backend decision still requires the real corpus, judged Portuguese queries,
filtered ANN recall, concurrent server latency, memory/index size, updates/deletes,
and recovery. No graph backend is selected without relationship-based query tests.

## Verification and rollout

Run core and isolated PostgreSQL integration tests:

```sh
OFL_RESEARCH_TEST_DATABASE_URL=postgresql://... uv run pytest -q
```

Each integration test creates and drops its own schema. The dedicated CI workflow
uses a disposable pgvector database and builds both apps; the public build reads
an explicitly synthetic snapshot generated by `tools/build_web_fixture.py`.

The code supports importing 1986 onward; that is **not** evidence that any real
historical files are loaded. Coverage reports received years independently from
the expected range. No source corpus is bundled in Git.

Before production: inventory real supplied inputs, measure one modern annual file
and representative archive assets, provision private storage/database backups,
configure gateway roles and Secrets, and run the same flows through the real gateway.
Use commit-tagged images and deploy through the private infrastructure repository.
Do not change public bucket policies or publish private text, previews or embeddings.
Do not vacuum Delta versions referenced by retained serving releases.

The production deployment can reuse a private Ollama service instead of installing
PyTorch in each API and worker. Set `OFL_RESEARCH_EMBEDDING_URL`, the exact model
name (including tag), and `OFL_RESEARCH_EMBEDDING_REVISION` to its full manifest
digest. The adapter checks that digest on every call and disables truncation.
`OFL_RESEARCH_EMBEDDING_THREADS` bounds Ollama's request-level CPU threads
(default 1, range 1–32). Keep the service warmup on the same thread setting to
avoid reloading the runner on the first search. This option preserves the model
tag/digest and existing vector namespace; it does not establish retrieval quality.
Ollama vectors remain in a separate model/revision namespace from Hugging Face
vectors, even when both models are called BGE-M3.
