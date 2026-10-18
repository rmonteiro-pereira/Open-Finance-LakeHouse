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
