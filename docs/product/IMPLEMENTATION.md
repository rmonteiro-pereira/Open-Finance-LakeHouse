# Sequential implementation ledger

User direction, 2026-10-18: implement the whole approved direction sequentially.
Personas remain hypotheses about demand, not interview results. Engineering and
synthetic acceptance are recorded separately from production and user outcomes.

| Order | Deliverable | Acceptance |
|---|---|---|
| 1 | Owned investigations, immutable revisions, findings, references, exports | ownership, optimistic concurrency, source validation, reproducible receipts |
| 2 | Multi-series preparation and transformations | pinned versions, explicit alignment/null rules, deterministic calculations |
| 3 | Investment portfolios and scenarios | exact valued positions, currency/identity guards, explicit assumptions |
| 4 | Bounded multi-step Inkwell research | search then context, grounded quotes, tailored clarification, source safeguards |
| 5 | Watchlists, checks, diagnostics and comparisons | identity-aware keys, immutable reruns, review history, scoped differences |
| 6 | Integrated dashboard, API/MCP, adversarial evaluation | usable task paths and failures, no unsupported claims of customer validation |

This ledger supplements the inherited 20-item backlog. Source readiness and
live model configuration are reported honestly; they do not replace implementing
the supported workflows. No generated investment recommendation or backtest is
claimed from descriptive data alone.

## Local verification

All six implementation groups now have code paths. Contracts are exercised by
`tests/test_product_workflows.py`, the existing research tests and Inkwell tests.
The actual private edge supports dependent passage reads and independent citation
verification; the original diagnostic baseline remains preserved. The new
`evidence/agent-diagnostic-sequential.json` records deterministic structural checks,
not live-model reasoning quality. Browser acceptance used a real local private API
and disposable PostgreSQL with synthetic news/snapshot fixtures. It completed
research/context → dossier, dataset preparation, portfolio shock and saved
check/schedule workflows. Source policy changes also redact restricted saved
captures without rewriting their immutable checksums.

The source bridge was separately exercised on an isolated schema with real private
macro and letter objects. The live corpus/pointer was unchanged. Deployment and
live-model acceptance are recorded separately; participant outcomes remain open.
