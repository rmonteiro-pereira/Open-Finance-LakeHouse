# Reproduce a dataset without OFL serving infrastructure

In the dataset workbench, prepare the desired pinned recipe, then select **Baixar
pacote para reproduzir offline**. The ZIP contains the input observations actually
used, their series definitions/source dataset checksums, the normalized recipe,
output manifest, resulting CSV, a checksum receipt and `reproduce.py`.

Extract into a new directory, inspect the script and run:

```sh
python3 reproduce.py
```

Python 3.11+ and its standard library are sufficient. The script independently
recomputes the five supported descriptive transformations at decimal precision 50,
aligns observation dates according to the recipe and compares the resulting bytes,
row count and SHA256 with the exported dataset. It creates `dataset.reproduced.csv`
and refuses to overwrite existing output. No network, PostgreSQL, model, pandas,
OFL installation or desktop backend is required after downloading the bundle.

All recipe/source bounds still apply: at most eight outputs, 100,000 source rows
per output and 100,000 aligned dates. A bundle is additionally bounded to 64 MiB
of uncompressed contents. Narrow the dates/series if the bound is exceeded.
Existing snapshot checksum, open-redistribution and authentication checks apply.
There is no arbitrary SQL or remote code generation.

The REST API exposes both `POST /api/v1/datasets/bundle` with the ordinary dataset
specification and read-only `GET /api/v1/datasets/bundle?specification=<JSON>`.
The GET route supports existing read-only service credentials; dashboard POSTs
retain their same-origin requirement. Both routes return the same ZIP without
creating a saved artifact or changing source data.

Python MCP adds `dataset_reproduction`, the fourteenth read-only tool. It validates
the recipe against the actual preview endpoint and returns a ZIP resource link.
Fetch that `ofl://datasets/reproduction/...` resource using MCP `resources/read`;
the server uses its existing service identity to download the ZIP. Binary contents
are transferred as a resource rather than embedded in the tool's explanatory text.
No credential is placed in the resource URI. Resource access revalidates source
availability and permissions; a previously returned link is not a permanent grant.

The bundle's captured observations preserve the source numeric representation;
decimal calculations cannot recover precision absent from the input. Frequency
alignment remains date-based with no imputation or hidden warm-up, not monthly
aggregation or proof of publication-time availability. Hashes detect changes to
the captured export; they are not signatures or independent confirmation that the
source is correct. These artifacts alone do not establish a valid backtest.

Acceptance distinguishes three claims: checksum verification, independent offline
recomputation of the captured recipe, and successful end-user reproduction. The
first two have automated contracts; the third still requires actual user trials.


## Production acceptance — 2026-10-18

Public #77 / 4bb2033 is deployed with all seven services ready; agent/model
configuration is unchanged and the desktop is not a serving dependency. The
combined main research CI passed 288 tests and all four runtime image builds.

A real 2020–2025 recipe joined monthly IPCA, daily effective Selic and USD/BRL,
including a descriptive FX percent-change column. It produced 1,536 rows with
explicit nulls: no monthly values were forward-filled onto daily dates. API
gateway POST, read-only service GET, native stdio MCP resource download and the
production browser delivered the same 80,999-byte ZIP. Anonymous access, wrong
origin and service POST remained denied. One export took 2.16 seconds; this is
a single integration observation, not a performance benchmark.

The captured recipe was recomputed first in an isolated Python process on the VPS
and then in a separate temporary client environment with `python -I -S`. Both
outputs matched the exported CSV byte-for-byte and its SHA256. The client verified
that the bundled reproducer matched the reviewed repository source before running
it. This uses the desktop as a temporary client, never an application backend.

Browser acceptance covered actual catalog selection, four output columns, date
filters, preparation, ZIP download, mobile layout and zero JavaScript errors.
It used the SSH loopback trusted application boundary with configured identity
and origin injected; it did not retest Tailscale authentication. No saved user
work was created or changed. See `evidence/reproduction-live-acceptance.json`.

These are builder-operated integration/reproduction results. The complete
agent-driven T02 task and actual participant reproduction/value remain open.
Analytical synthesis (T01), independent source relevance and source completeness
remain separate unresolved gates in DELIVERY-GAPS.md.
