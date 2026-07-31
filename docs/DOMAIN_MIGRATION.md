# Domain migration — `vanir-proxmox.duckdns.org` → `vanir.dev.br`

The repo used to hardcode the DuckDNS dynamic-DNS name `vanir-proxmox.duckdns.org` for every
cluster-hosted service. The homelab now serves everything under the owned domain
**`vanir.dev.br`**, so the DuckDNS names are gone from the tree.

## How the new hostnames were derived

They were **not** guessed. Every replacement below was read from a live Ingress object:

```bash
kubectl get ingress -A -o wide
```

Only hostnames that a real Ingress actually serves appear in this repo.

## Old → new mapping

| Service | Old hostname | New hostname | Ingress (ns/name) |
|---|---|---|---|
| Grafana | `grafana.vanir-proxmox.duckdns.org` | `grafana.vanir.dev.br` | `monitoring/grafana` |
| Airflow | `airflow.vanir-proxmox.duckdns.org` | `airflow.vanir.dev.br` | `airflow/airflow-ingress` |
| Spark | `spark.vanir-proxmox.duckdns.org` | `spark.vanir.dev.br` | `spark/spark` |
| LakeFS | `lakefs.vanir-proxmox.duckdns.org` | `lakefs.vanir.dev.br` | `lakefs/lakefs` |
| MinIO console | `minio-ui.vanir-proxmox.duckdns.org` | `minio-ui.vanir.dev.br` | `minio/minio-console-ingress` |
| MinIO S3 API | `minio-api.vanir-proxmox.duckdns.org` | `minio-api.vanir.dev.br` | `minio/minio-api-ingress` |
| Base domain (prose) | `vanir-proxmox.duckdns.org` | `vanir.dev.br` | — |

At the time of this migration, other Ingresses existed in the cluster but were not referenced by
this repo, so nothing was changed for them: `dashboard.vanir.dev.br`, `openbao.vanir.dev.br`,
`opencost.vanir.dev.br`, `fluxo.vanir.dev.br`, `powersync.vanir.dev.br`. (OpenMetadata was in that
list too; the access runbook has since been corrected to mention it — see
[the follow-up below](#follow-up-the-access-runbook-no-longer-hardcodes-hostnames-at-all).)

## Services that could **not** be mapped

### Dremio — not deployed

`dremio.vanir-proxmox.duckdns.org:9047` appeared in `conf/base/dremio.yml` and
`docs/ENVIRONMENT_SETUP.md`. There is **no Dremio ingress, service, or namespace in the cluster**
(`kubectl get ingress/svc -A` and `kubectl get ns` all come back empty for Dremio). Inventing
`dremio.vanir.dev.br` would have documented a hostname that resolves to nothing, so instead the
default fell back to the local Dremio port:

```yaml
endpoint: ${env:DREMIO_ENDPOINT,http://localhost:9047}
```

It stays an env-var override (`DREMIO_ENDPOINT`), so once Dremio is deployed and given an Ingress,
point that variable at the real host — and update this file. `docs/ENVIRONMENT_SETUP.md` already
flags Dremio as optional and not yet configured in the cluster.

## Files touched

Hostname strings only — no restructuring, reformatting, or unrelated edits.

- `GUIA_ACESSO_FERRAMENTAS.md`
- `README_ENV_SETUP.md`
- `conf/base/lakefs.yml`, `conf/base/observability.yml`, `conf/base/dremio.yml`
- `docs/ENVIRONMENT_SETUP.md`, `docs/CONFIGURING_GIT_SYNC_FOR_AIRFLOW.md`,
  `docs/DEPLOYING_DAGS_TO_AIRFLOW.md`, `docs/TESTING_AIRFLOW_DAGS.md`,
  `docs/TESTING_PIPELINE_END_TO_END.md`, `docs/SCREENSHOTS_TODO.md`
- `scripts/setup_env.sh`
- `notebooks/lakehouse_data_visualization.ipynb` (a stored cell output printed the old endpoint)

Verification: `git grep -i duckdns` returns nothing outside this file, which necessarily names the
old hostnames in order to record the mapping. To check the rest of the tree:

```bash
git grep -i duckdns -- ':!docs/DOMAIN_MIGRATION.md'
```

## Follow-up: the access runbook no longer hardcodes hostnames at all

Three inaccuracies were noticed while migrating and were out of scope for a hostname-only
change. They were all resolved later, when `GUIA_ACESSO_FERRAMENTAS.md` was rewritten as a
**generic runbook** — it now carries placeholders (`<BASE_DOMAIN>`, `<INGRESS_IP>`,
`<NODE_IP>`) and `kubectl` lookups instead of literal addresses, so it cannot drift again:

1. **Ingress controller IP.** The runbook used to name a specific LAN address, which had since
   changed. Private-range IPs are no longer published in this repo; the current one comes from
   `kubectl get svc -n ingress-nginx -o wide`.
2. **OpenMetadata exposure.** The summary table said "Port-forward necessário / Não exposto via
   Ingress"; `openmetadata/openmetadata` does serve an Ingress today. The table now points at
   `openmetadata.<BASE_DOMAIN>` and tells the reader to confirm with `kubectl get ingress`.
3. **Scheme.** No Ingress in the cluster declares a `spec.tls` block — they all listen on port 80.
   The runbook now shows `http://` consistently and lists TLS via cert-manager as a hardening
   item rather than implying it is already in place.
