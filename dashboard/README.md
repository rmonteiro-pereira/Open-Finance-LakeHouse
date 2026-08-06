# Ledger — OFL Lakehouse dashboard

A financial-editorial BI dashboard over the **Open-Finance-LakeHouse** gold marts:
Brazilian macro, real interest, inflation, FX, the treasury yield curve and equities.

Built with **Next.js 16** (App Router) · **Tailwind v4** · **shadcn/ui** · **Recharts**.
Dark, terminal-meets-editorial aesthetic (Fraunces / Hanken Grotesk / JetBrains Mono).

## Quick start

```bash
python snapshot/gen_synthetic.py   # required: generates public/data/*.json
pnpm install
pnpm dev                           # http://localhost:3000
```

`public/data/*.json` is a **derived artifact and is not versioned** — a clean clone has no
data until you generate it. That is deliberate: the snapshot the repo used to ship was
**synthetic**, and committed synthetic data is indistinguishable from real data to anyone
reading the repo. See the RFC (`_specs/rfc-ofl-produto-de-dados.md`): from F4 on, this
dashboard reads the **public release**, not MinIO and not a mock.

## Data

The dashboard reads JSON snapshots of the gold marts from `public/data/*.json`.
There are two interchangeable producers writing the **identical shape**:

| Script | Source | Use |
|---|---|---|
| `snapshot/gen_synthetic.py` | realistic mock | local dev, Vercel demo, screenshots |
| `snapshot/export.py` | live Delta via DuckDB `delta_scan` on MinIO | cluster / real data |

```bash
# regenerate the mock snapshot
python snapshot/gen_synthetic.py

# pull the live snapshot (needs MinIO creds — see .env.example)
MINIO_USER=… MINIO_PASSWORD=… MINIO_ENDPOINT=http://localhost:9000 \
  python snapshot/export.py
```

Loaders live in `src/lib/data.ts` (typed, server-only, read at build/ISR).
Marts and their columns are documented in `../docs/DASHBOARD_HANDOFF.md`.

## Pages

`/` macro overview · `/real-interest` · `/inflation` · `/fx` · `/yield-curve` ·
`/equities` · `/catalog`.

- **`/catalog`** lists **all 48 registry series** (`dim_series`), grouped by domain and
  searchable. The 40 single-value series draw a sparkline + latest reading straight from
  `fact_observation`; the 8 multi-symbol facts (treasury / security_price) link to the page
  that plots them.
- **`/equities`** carries a curated interactive watchlist **and** the **whole B3 cash market**
  — every listed round-lot name (`mart_equity_universe`), searchable and filterable by sector.

All pages are **ISR** (`revalidate = 3600`), so a fresh snapshot is picked up within the hour
without a redeploy.

## Deploy

### Vercel
Import the repo, set the project root to `dashboard/`. Zero backend — but the build needs
`public/data/*.json` to exist, and it is no longer committed: the deploy step must run a
producer first (`gen_synthetic.py` for a demo, `export.py` for cluster data, the release
fetch from F4 on). `vercel.json` pins the framework and install command.

### Cluster (Kubernetes)
```bash
docker build -t ghcr.io/rmonteiro-pereira/ofl-dashboard:latest .
docker push ghcr.io/rmonteiro-pereira/ofl-dashboard:latest

kubectl create configmap ofl-dash-export \
  --from-file=export.py=snapshot/export.py -n default
kubectl apply -f deploy/k8s/dashboard.yaml
```
`deploy/k8s/dashboard.yaml` provisions the Deployment, Service, Ingress
(`dashboard.vanir.dev.br`) and an hourly `ofl-dash-refresh` CronJob that runs
`export.py` against MinIO into a shared volume. The image bakes the synthetic
snapshot as a first-boot fallback. Requires the `minio-creds` secret in `default`.

## Notes
- Synthetic data is real-*shaped*, not real. ANBIMA series are sandbox even when live.
- `dim_series.unit` differs by series; never mix units on one axis.
