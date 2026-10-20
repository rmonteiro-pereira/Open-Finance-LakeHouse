# Contributing

The bar here is "a repo I would be happy to inherit": it runs from a clean clone, the tests
mean something, and the reasoning is written down.

## Setup

Requires **Python 3.11+**. The core install is deliberately light — Polars and DuckDB only —
with the heavy lanes behind extras.

```bash
git clone https://github.com/rmonteiro-pereira/Open-Finance-LakeHouse.git
cd Open-Finance-LakeHouse
pip install -e ".[dev]"
```

`uv.lock` is committed. Please do not commit a re-resolved lock file unless changing a
dependency is the point of the change.

Optional extras, installed only when you need that lane: `spark`, `streaming`, `airflow`,
`yahoo`, `lineage`.

## Tests

```bash
pytest -q          # 134 tests, offline, a few seconds
```

These are the tests CI runs. They need **no cluster, no MinIO, no Spark session, no Airflow
and no API keys** — that constraint is deliberate, and it is what makes the green badge worth
looking at. `.github/workflows/tests.yml` installs base + `dev` only.

**Keep it that way.** A test that needs a network service, a credential or a running cluster
belongs behind an extra and a marker, not in the default run.

### Mutation gate (research)

The research functions that decide access, money and evidence integrity have a mutation
gate: each mutant of those functions must make an offline test fail.

```bash
uv run --extra dev --extra research --with mutmut==3.3.1 python tools/mutation_gate.py
```

| Scope | Functions | Target |
|---|---|---|
| access | `api.authenticate`, `require_admin`, `require_reviewer`; source-policy redaction in `workspaces.load` | 95 % |
| money | `query.per_unit`; `analytics.number`, `portfolio`, `compare_quotes`; COTAHIST `parse_record`, `stage`; table review `normalize`, `observation_period` | 95 % |
| evidence | `reference_review.reference_reviews`, `text_hash`; `dataset_bundle.bundle`; `dataset_replay.replay` | 95 % |
| other | `workspaces.evidence`, `validate`; `analytics.release_diff`, `prepare_dataset` | 85 % |

The command exits 1 when a scope is below target, and 2 when the mutmut run itself failed,
its metadata is missing, or a configured function produced no mutants. `-v` lists every mutant that was
not killed and each equivalent mutant with its reason; `--idle-tests` lists tests that touch
a gated function but kill no mutant. Equivalent mutants are either source-identical to the
original or listed, with the reason, in `EQUIVALENT` in `tools/mutation_gate.py`; mutant
numbers change when a function changes, so review that table after editing a gated function.

The gate runs the offline tests in `tests/test_research_access.py`,
`tests/test_research_money.py`, `tests/test_research_evidence.py` and the other research test
files. PostgreSQL tests are skipped during the run (the Delta writer they use does not survive
mutmut's per-mutant fork); the offline tests cover the gated logic directly, replacing the
database with recorded rows. The run uses half the cores at nice 10 and waits while the load
average exceeds the core count. Set `MUTATION_WORKERS` to change the worker count.

## Lint

```bash
ruff check .
```

Be aware: this currently reports a substantial backlog against the project's own rule
selection, so it is **not** yet a CI gate. If you are touching a file, leaving it cleaner than
you found it is welcome; a repo-wide reformat should be its own pull request.

## Adding a data source

The registry is the point of the design:

- A new **series** from a source that already has a handler is a one-entry change in
  `sources/registry.yml`.
- A new **kind** of source is one new handler.

Counts in documentation must be derived from `load_registry()`, never hand-counted — the DAG
docstring and the README both state numbers that have to stay true.

## Pull requests

- Branch from `main`; never commit to it directly.
- [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `docs:`,
  `chore:`, `test:`, `refactor:`.
- Explain **why** in the body. The diff already says what.
- One concern per PR.
- `pytest -q` green before opening.

## What not to commit

Nothing over 5 MB. No data, no `mlruns/`, no `.venv`, no notebook output blobs, no generated
HTML. No credentials, private hostnames or internal IP addresses — including in docs and in
committed notebook cell output.
