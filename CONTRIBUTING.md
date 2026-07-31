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
`yahoo`, `lineage`, `dbt`.

## Tests

```bash
pytest -q          # 119 tests, offline, a few seconds
```

These are the tests CI runs. They need **no cluster, no MinIO, no Spark session, no Airflow
and no API keys** — that constraint is deliberate, and it is what makes the green badge worth
looking at. `.github/workflows/tests.yml` installs base + `dev` only.

**Keep it that way.** A test that needs a network service, a credential or a running cluster
belongs behind an extra and a marker, not in the default run.

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
