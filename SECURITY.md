# Security Policy

## Reporting a vulnerability

Please report security issues **privately** rather than opening a public issue.

- Use GitHub's [private vulnerability reporting](https://github.com/rmonteiro-pereira/Open-Finance-LakeHouse/security/advisories/new) — preferred.
- Or email **rmonteiropereira1@gmail.com** with `SECURITY` in the subject.

Include the commit, the lane involved (ingest, silver, gold, streaming or orchestration), the
steps to reproduce, and what you observed. Expect an acknowledgement within **7 days**; this
is a personal project, so treat that as best effort rather than a guarantee.

## What this project does and does not hold

This lakehouse ingests **public** Brazilian financial and macroeconomic data — BACEN, IBGE,
IPEA, B3, Tesouro Direto, ANBIMA. It holds no personal data and no customer data.

**No credentials belong in this repository.** Configuration is by environment variable and
Kubernetes secrets. The cluster, its hostnames and its addresses are private infrastructure
and are deliberately not described here.

If you find anything credential-shaped committed — a key, a token, a password, a private
hostname or an internal IP address, **including inside documentation, notebooks or committed
cell output** — please report it through the private channel above rather than opening an
issue.

## Areas worth reporting

- **Injection through registry-driven code paths.** `sources/registry.yml` drives ingestion,
  DAG generation and the catalog. A crafted registry entry reaching a shell, a SQL string or
  a file path would be a real finding.
- **Path traversal** in the bronze/silver/gold write paths or the DuckDB export.
- **Deserialisation** of crafted Parquet or Delta input.
- **Airflow DAG parsing** — anything that executes at parse time rather than task time.
- **Dependency vulnerabilities** reachable from the pipeline entry points.

## Out of scope

- Availability or correctness of the upstream public data sources.
- The ANBIMA sandbox, which returns format-real but fictitious values by design.
- Resource exhaustion from deliberately configuring a local Spark job beyond the host.

## Supported versions

Fixes land on `main`. There are no maintained release branches.
