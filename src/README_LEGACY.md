# ⚠️ Legacy (deprecated) — Kedro implementation

The code under `src/` (the Kedro project) is **superseded** by the redesigned
package at `ofl/`. See `docs/architecture/redesign.md`.

It is intentionally **kept, not deleted**, until the new stack is validated on the
cluster (Spark silver + Airflow Asset DAGs running green against MinIO). Deleting
the only proven implementation before the replacement is verified would be unsafe.

**Do not build on this code.** New work goes in `ofl/`.

### Note on the legacy source handlers
Three legacy "handlers" returned **synthetic/mock data**, not real API results, and
were deliberately **not** ported:

- `utils/b3_api.py` — `_generate_mock_b3_data()` (random)
- `utils/anbima_api.py` — `generate_sample_anbima_data()` (synthetic)
- `utils/ipea_receita_api.py` — `_generate_mock_ipea_receita_data()` (random)

In the registry these are marked `status: planned`; they need real API integration
(ANBIMA/B3 require authenticated/paid feeds; IPEA has a real OData API) before being
activated.

### Removal plan
Once `ofl silver` and the Asset DAGs run green on the cluster and gold parity is
confirmed, delete `src/`, `conf/` (Kedro), and the Kedro deps, then drop this file.
