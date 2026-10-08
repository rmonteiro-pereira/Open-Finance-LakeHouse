# Open-Finance-LakeHouse — gotchas

## Digest
- **Never commit or push to `main`, never force-push, never merge a PR.** Branch,
  then PR. Other checkouts of this repo may hold the owner's uncommitted work: only
  `git add` the exact paths you wrote.
- **`ofl_silver` waits for all ten source DAGs.** It runs once per ingest wave (it used
  to run on *any* bronze asset, three or four full merges a night). Consequence: after
  re-running one source by hand, trigger `ofl_silver` by hand as well, or set
  `OFL_SILVER_TRIGGER=any`. Each run still re-merges all of bronze (about 30 minutes).
- **`b3_cotahist` hard-codes `years: [2024, 2025]`.** It re-downloads both annual
  archives every day and never fetches 2026.
- **Image builds are not lock-pinned.** They `uv pip install "."`, so versions float
  inside the `pyproject.toml` ranges; `delta-spark` can resolve to 3.3.x against the
  baked 3.2.1 jar.
- **The image tags are mutable** (`:slim`, `:spark`) and no pull policy is set, so a
  node keeps a stale image until it is removed. No CI publishes them.
- **`ghcr-pull` is always attached** as an image pull secret. If it is missing the
  kubelet only warns, but the image must then already be on the node.
- **No timeouts anywhere.** No `execution_timeout`, no `startup_timeout_seconds`
  (provider default 120 s). A slow first image pull can eat the retries.
- **Task-failure alerting is a Pushgateway metric only** (`_alerts.py`), and a no-op
  unless `OFL_PUSHGATEWAY_URL` is set. There is no Slack, webhook or email.
- **`is_delete_operator_pod` is deprecated** in the Kubernetes provider; a newer
  provider may reject it. Not checked against current provider versions.
- **Ingest needs internet egress** to the data sources. The image comment "pods have
  no external egress" is about build-time baking, not ingestion.
- **Docs that are stale:** `docs/architecture/redesign.md` describes 6 per-domain
  DAGs (the code generates per-handler DAGs); `docker-compose.yaml` is legacy local
  dev and its DAG mount does not match `orchestration/airflow/dags`.
- **ANBIMA** needs `ANBIMA_CLIENT_ID` and `ANBIMA_CLIENT_SECRET` in an optional
  Secret (`anbima-creds`); without it only those 4 tasks fail.
