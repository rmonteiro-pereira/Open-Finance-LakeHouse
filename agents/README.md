# OFL research agents using Inkwell

Three actual Inkwell specialists: **financial research**, **data and methods**, and
**data quality**. Tools read OFL's private REST API; no SQL, shell, arbitrary URL,
filesystem, upload, approval, retry or publication tool is available.

Quotes require explicit symbol, market and dates. Monetary values stay exact
strings, with currency, quotation factor and release. Archive search retains
item/passage/asset/page references and embedding revision. Catalog dates are
publication intervals. OCR is not approved numerical data. Review-history reads
still require the caller's reviewer/admin role at the OFL API.

Each turn allows six model steps and twelve tool calls. Dependent reads receive
bounded source observations. Final excerpts are independently re-read through
the authenticated API and checked as exact substrings. Optional qualitative
interpretations are model hypotheses for human review; citation validation does
not verify entailment. Unmatched numerical claims are excluded. There is no
production scripted-data fallback.

## Install and test

Inkwell is private. Consume approved tarballs without committing its implementation
or archives to this public repo or publishing container images containing them.

```sh
cd agents
bun tools/install.ts /path/to/approved/inkwell/dist/packages
bun run typecheck
bun test
OFL_AGENT_TEST_DATABASE_URL=postgresql://user:password@localhost/research_test bun test
```

The helper copies four approved runtime packages into ignored `vendor/`; the
lockfile pins their content. The Dockerfile needs this prepared private context.

## Configure

Supply server-side settings through the private infrastructure secret store:

| Variable | Purpose |
| --- | --- |
| `OFL_RESEARCH_DATABASE_URL` | Dedicated research database; never Airflow metadata |
| `OFL_RESEARCH_API_URL` | Internal OFL API base URL |
| `OFL_RESEARCH_GATEWAY_TOKEN` | Existing private gateway secret |
| `OFL_RESEARCH_ORIGIN` | Exact dashboard origin for CSRF checks |
| `OFL_AGENT_MODEL_BASE_URL` | Approved text-generation endpoint supporting chat/tools |
| `OFL_AGENT_MODEL_API_KEY` | Generation endpoint credential |
| `OFL_AGENT_MODEL_NAME` | Explicit model; no implicit paid default |
| `OFL_AGENT_NO_THINK` | `1` disables thinking explicitly in native Ollama and adds the Qwen instruction; default off |
| `OFL_AGENT_MODEL_PROTOCOL` | `ollama` selects native `/api/chat`; otherwise OpenAI compatible |
| `OFL_AGENT_MODEL_TIMEOUT_MS` | Per-call budget, default 60000, maximum 180000 |
| `OFL_AGENT_TURN_TIMEOUT_MS` | Whole-request deadline, default 110000, maximum 600000 |
| `OFL_AGENT_HOST` / `OFL_AGENT_PORT` | Defaults `127.0.0.1:8082` |

Run `bun start`. Missing configuration refuses startup. Existing BGE-M3 embeddings
cannot power chat turns. Add `OFL_RESEARCH_AGENT_URL` to the private dashboard,
pointing at this isolated service. Keep Next.js behind the trusted Tailscale
Serve gateway; it replaces caller-supplied identity headers. No public ingress.

Agent authentication revalidates `/api/v1/me` every request, verifies the gateway
secret and enforces Origin on mutations. Conversation ownership is enforced by
Inkwell. PostgreSQL schemas `ofl_agent_market`, `ofl_agent_archive`, and
`ofl_agent_quality` separate specialists and survive restarts. At 256 cached
identity/specialist pairs, new pairs fail closed instead of evicting active turns.

## Routes and UI

`GET /api/agents` returns the catalog. Inkwell's conversation, turn, retry,
transcript and streaming routes mount under `/api/agents/{market|archive|quality}`.
The private dashboard **Pesquisa** tab has specialist selection, saved
investigations, source tables, original-file links, handoff carrying references,
and Markdown/JSON exports. Handoff prepares a new question for explicit submission;
it preserves the original saved investigation.

## REST and MCP

The existing `/api/v1` API gains authenticated `/openapi` and `/releases` routes.
Release listings omit internal storage paths. Configure per-principal machine
credentials using `OFL_RESEARCH_SERVICE_TOKENS`, a JSON identity-to-token mapping.
Tokens must have at least 32 characters; principals must exist in
`OFL_RESEARCH_ROLES`. Machine credentials are read-only even for human admins.
Removing a token/role revokes access after configuration reload.

`ofl research mcp` exposes stdio tools `coverage`, `quotes`, `search_archive`,
`item`, and `releases`. Set `OFL_RESEARCH_API_URL` and `OFL_RESEARCH_API_TOKEN` to
a private reachable API and issued service credential. The adapter makes bounded
allowlisted GETs only. Never distribute the shared gateway secret to MCP clients.

Local tests do not establish a live deployment, actual generation-model answers,
full archive ingestion, or completion of the remaining 20-item product backlog.

Current checks: 9 Inkwell tests (including real PostgreSQL restart/ownership),
202 Python tests passed with 3 optional OCR/pilot skips, real stdio MCP discovery,
production Next.js build, a local private container bootstrap with PostgreSQL and
401 rejection, and a browser flow covering exact evidence, reopen, export and
handoff. Browser model/source data were scripted fixtures, not live generation.

## Workbench foundation

The archive tool catalog now includes indexed news and manager letters through
`search_archive`, cited passage inspection through `read_passage`, and macro
catalog/series reads through `read_data_catalog` and `read_macro_series`. Market
specialists also receive the macro tools. These connections support bounded multi-step research. Direct search and CSV export in
the workbench remain available without a text-generation provider.

See [the workbench operation guide](../docs/research/WORKBENCH.md) for ingestion,
source restrictions and reproducible exports. The Python MCP exposes thirteen
read-only tools, including `data_catalog`, `macro_series` and `passage`.

Native Ollama uses the [documented chat API](https://docs.ollama.com/api/chat) with
explicit `think:false`, bounded output, preserved argument strings and cancellation.
Text instructions alone did not prevent thinking in the tested compatibility path.
When increasing CPU budgets, set `OFL_AGENT_REQUEST_TIMEOUT_MS` on the private web
proxy above the agent turn deadline. The UI supports canceling a running request.

CPU deployments may set `OFL_AGENT_MAX_TOKENS` (128–2048, default 2048).
Task routing advertises only relevant registered read tools to reduce prefill
latency; it is not an authorization boundary and does not enable writes.


## Guided document research

`OFL_AGENT_GUIDED_DOCUMENTS=1` uses a source workflow for news, manager letters and
document questions: bounded searches, context reads, independent canonical
verification, then one optional model call for qualitative hypotheses. Code plans
source collection; it does not invent source data or pretend these steps were
model inference. Empty coverage and unusable generated interpretations are reported
explicitly. The final output separates exact source excerpts from model drafts.
Numeric/data/quality tools retain their read-only contracts. Conversation sources
can be handed directly to an owned dossier without copying internal identifiers.
