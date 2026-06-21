# ⚠️ Legacy (deprecated) — Kedro implementation

The code under `src/` (the Kedro project) is **superseded** by the redesigned
package at `ofl/`. See `docs/architecture/redesign.md`.

It is intentionally **kept, not deleted**, until the new stack is validated on the
cluster (Spark silver + Airflow Asset DAGs running green against MinIO). Deleting
the only proven implementation before the replacement is verified would be unsafe.

**Do not build on this code.** New work goes in `ofl/`.

### Note on the legacy source handlers
Three legacy "handlers" returned **synthetic/mock data**, not real API results
(`utils/b3_api.py`, `utils/anbima_api.py`, `utils/ipea_receita_api.py`). They were
**reimplemented for real** in `ofl/ingestion/` rather than ported:

- **IPEA** (`ipea.py`) — real ipeadata OData4 API (no auth). **Active.**
- **B3** (`b3.py`) — Ibovespa (`^BVSP`) via Yahoo; B3 has no free official feed. **Active.**
- **ANBIMA** (`anbima.py`) — real OAuth2 Feed API (secondary-market TPF). Implemented
  and wired, but **`status: planned`** until registered credentials are provided
  (`ANBIMA_CLIENT_ID` / `ANBIMA_CLIENT_SECRET`, via OpenBao `secret/claude/anbima`).

### Removal plan
Once `ofl silver` and the Asset DAGs run green on the cluster and gold parity is
confirmed, delete `src/`, `conf/` (Kedro), and the Kedro deps, then drop this file.
