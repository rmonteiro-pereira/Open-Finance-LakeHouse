# 0001 — Why the mart layer has no dbt, SDP or SQLMesh

- **Status:** Accepted
- **Date:** 2026-07-31

## Context

The gold layer is SQL files in `ofl/transform/gold/models/` executed by a 147-line runner,
`ofl/transform/gold/runner.py`. The runner opens DuckDB, registers the silver Delta tables as
views (`register_silver`), runs each model, and writes the result back as gold Delta through
delta-rs (`execute_models`). The model list is flat and the runner performs no dependency
resolution, because there is nothing to resolve — the comment above `MODELS` states the design
directly: *"Marts are independent (no inter-mart refs)."*

A dbt lane also existed, and was removed (#35). It had never been part of the pipeline: its own
README opened with *"This is a parallel demonstration lane. It is not the production path,"* and
went on to say the lakehouse *"already proves the modelling capability dbt is usually a proxy
for… with its own DuckDB SQL runner."* It read a snapshot export, never touched MinIO and never
ran in Airflow.

Its removal leaves the mart layer with no branded transformation framework at all, and that is
the decision this record exists to make legible. An unexplained absence reads as a gap — a
reader assumes the tool was never considered. It was considered, twice, and rejected on the
merits both times.

Three properties of this repository decide it:

- **The model set is flat and ref-free.** No mart reads another mart; every model reads silver
  views. A framework whose central abstraction is `ref()` and a resolved DAG has an empty graph
  to manage here. The dependency handling in `runner.py` is trivial *by construction*, not by
  neglect.
- **DuckDB rebuild economics.** Marts are full-rebuild overwrites (`write_deltalake(...,
  mode="overwrite")`). The largest input fact is `fact_derivatives_quote`, at single-node scale —
  small enough that recomputing everything is simpler and safer than reasoning about what a new
  row invalidated. That is a scale judgement, not a measured constant, and this repo deliberately
  publishes no row count for it (the value moves with every backfill window). To check it against
  a live lakehouse rather than take it on faith:

  ```bash
  # needs read access to the bucket; same delta_scan path the gold runner uses
  duckdb -c "SELECT count(*) FROM delta_scan('s3://<bucket>/silver/fact_derivatives_quote')"
  ```

  The decision below only depends on the answer being *small* — if that count ever approaches the
  point where a full rebuild stops fitting in one node's heap, this ADR is the thing to revisit.
  The streaming lane reached the same conclusion independently and wrote down why
  (`ofl/streaming/mart.py`): its derived columns are all *relative* — rolling windows, returns
  against the previous bar — so the last rows change on every pass and incrementality would buy
  complexity, not time.
- **Declarativity already lives in Spark, at silver.** The expensive, stateful, genuinely
  incremental step is the conformance MERGE in `ofl/transform/spark/silver.py`, which is an
  idempotent Delta `merge(...).whenMatchedUpdateAll().whenNotMatchedInsertAll()`. That is where
  a declarative engine earns its keep, and it is already there. Gold is a set of independent
  SELECTs over the result.

## Decision

**The mart layer is plain SQL models run by the existing DuckDB runner, hardened with post-build
checks.** No transformation framework is adopted.

Checks follow the house convention rather than importing a new one: a check is a SQL file that
returns violating rows, run against the same open DuckDB connection that just built the mart, in
the pod that built it. A returned row fails the gold task, pushes a data-quality metric through
the existing `ofl/platform/metrics.py` path, and withholds the `gold/marts` asset — the same
control the Pandera contract already applies at ingest (`ofl/ingestion/landing.py`), applied at
the other end of the pipeline.

This is the shape both lanes already converged on — SQL files plus a thin runner, batch and
streaming — and keeping one shape is worth more here than any framework's ecosystem.

## Alternatives rejected

**dbt.** Rejected and removed. It duplicated a runner this repo already had, over a model set
with no `ref()` graph for it to order. Its own README conceded the point: a parallel
demonstration lane, not the production path. Its generic tests were also weaker than what is
here — `tests/test_gold_marts.py` pins the arithmetic against hand-calculated outputs, not
merely null-ness. The two things it did contribute that nothing else had — the DI curve-grid and
slope analytics, and assertions against *real materialized* data — are features and controls,
not tooling, and are being recovered as plain SQL models and as the post-build checks above.

**SQLMesh.** Rejected because it solves two problems this repo provably does not have.
It exists for incremental-model correctness and for virtual environments with plan/apply
promotion. Incrementality where it matters is already handled by the Spark Delta MERGE at
silver; the marts are deliberate full rebuilds of a single-node worst case; and there is one
environment. Adopting it would add a state database, a promotion model, and a scheduler concept
sitting beside Airflow assets — to rebuild a handful of independent SELECTs.

**Spark Declarative Pipelines (SDP).** Rejected on maturity and on fit. SDP is the open-source
incarnation of Databricks' declarative pipelines, shipped in Apache Spark 4.1 and **months old
at the time of writing**. In a public repository, a lane built on immature tooling that then
quietly rots is worse than not having the lane: the reader sees the rot, not the ambition. On
fit, it is a declarative *pipeline* framework — expectations, incremental processing, a managed
dependency graph — and the mart layer has no graph and no incrementality to declare. Its real
attraction was resemblance to a production stack elsewhere, and resemblance is not a reason for
this repository to carry machinery it has no work for. Where that experience is true at
production stakes, it belongs stated from there, not re-enacted here over a set of independent
SELECTs.

**Any second data-quality framework (Great Expectations, Soda) for the post-build checks.**
Rejected: Pandera already owns data quality at ingest. A second framework would double the
surface for one pipeline, and both express as a context/suite/YAML stack what
`SELECT … FROM mart WHERE <violation>` expresses in the language the models are already written
in, in the engine that is already open.

**Doing nothing and saying nothing.** Rejected — this record is the rejection. The engineering
case for restraint was strong enough on its own; the perception case was not, because restraint
and oversight are indistinguishable from the outside unless one of them is written down.

## Consequences

- No framework to install, pin, upgrade, or explain. The mart layer is readable end to end in
  one 147-line file plus the SQL beside it, and it is the same shape as the streaming mart.
- The checks are a bespoke micro-framework in embryo, and that is the honest cost. Severity
  levels, stored failure rows, generated docs, and `ref()`-style ordering would each have to be
  hand-built if they were ever needed, without an ecosystem behind them. The sum of enough of
  those hand-builds is a worse dbt. The reversal condition below exists precisely so that this
  is caught as a threshold crossing rather than accumulating one exception at a time.
- The cross-lane second opinion the dbt lane provided — an independent recomputation of the
  IPCA compounding, asserted to agree with the production mart — is not restored by this
  decision. Invariant checks on one computation are not an independent recomputation of it.
  That redundancy was real value, traded away against the cost of maintaining a second lane, a
  second environment, and a snapshot-export workflow. It is a judgement call, not a proof.
- Until the post-build checks land, `execute_models` writes with `mode="overwrite"` and performs
  no post-write validation, and `skip_on_error` deliberately swallows an absent upstream. A mart
  that computes garbage from real data therefore publishes silently. That gap is the specific
  thing this decision commits to closing; it is not an argument for a framework, because no
  framework was preventing it either.

## Reversal condition

Any one of these should reopen the question. Each is stated so that it can be checked rather
than argued:

1. **A mart reads another mart.** The moment a model in `ofl/transform/gold/models/` selects
   from a gold table instead of a silver view, the flat `MODELS` list stops being a correct
   execution plan and the runner needs a real resolver — and the comment at `MODELS` becomes
   false. This is decidable from the SQL and cheap to assert in `tests/test_gold_marts.py`;
   asserting it is the recommended way to make this trigger fire on its own. Hand-building
   ordering, then severity, then docs, is how a project ends up maintaining a private dbt.
2. **The full rebuild stops being cheap.** Concretely: the `build_gold_marts` task in the
   `ofl_gold` DAG exceeding **15 minutes**, or any single mart exceeding **5 minutes**, on the
   `:slim` pod it already runs in (`SLIM_IMAGE` in `orchestration/airflow/dags/ofl_dags.py`). That
   duration is already recorded per run by Airflow, so no new instrumentation is needed to
   observe it. At that point full-overwrite marts are no longer obviously right, incremental
   materialization becomes a genuine question, and a tool built around it — SQLMesh being the
   closest fit — is worth a real evaluation. Below that threshold, incrementality is complexity
   bought for nothing.
3. **A second consumer needs governed metrics.** Today there is exactly one consumer, a
   dashboard reading gold Delta directly (`docs/DASHBOARD_HANDOFF.md`), so a metric definition
   has no one to be negotiated with. The trigger is a second consumer computing a metric this
   repository also publishes — the observable event being the two disagreeing on a number that
   is supposed to be the same. A semantic layer answers a dispute between consumers; with one
   consumer there is no dispute, only a contract with itself.

A framework becomes the right call when it manages complexity this code cannot. Three
independent thresholds are written down here so that the day it does, the decision is a
re-reading of this record and not a rediscovery.
