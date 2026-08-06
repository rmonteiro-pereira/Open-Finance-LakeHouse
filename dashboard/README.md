# Ledger — the OFL reader surface

Six routes, each named after a question somebody asks out loud:

`/` Como está o Brasil hoje · `/juro-real` · `/inflacao` · `/curva-do-tesouro` ·
`/serie/[series_id]` · `/confianca`

The eight routes before these were named after marts — `/fx`, `/yield-curve`,
`/derivatives` — which is the factory floor plan projected onto the shop window. Nobody
arrives wanting `mart_real_interest`; they arrive asking whether the real rate is high.

`/derivatives` and `/equities` are gone for a second, independent reason: B3 barred
redistribution of derived values absent written authorisation, so those numbers cannot
appear on a public page at all.

## Data

**One source: a published release.** Not MinIO, not a mock.

```bash
ofl release build --from tests/fixtures/release --to ../out   --release-id 1970-01-01.1 --release-class fixture      # from the repo root
python snapshot/from_release.py --release ../out          # writes public/data/*.json
pnpm install && pnpm dev
```

Both previous producers are gone, and the removals are the point:

- `gen_synthetic.py` generated a realistic mock, and shipping it as the default meant the
  site's out-of-the-box state was invented numbers wearing the same chrome as real ones.
- `export.py` read Delta from MinIO, which made the reader surface a dependent of the
  homelab. A surface that needs the cluster dies with the cluster.

Consuming the same artefact an external consumer does is also what lets this dashboard be
the release's **first real consumer** — proving the interface by use instead of assertion.

`public/data/*.json` is derived and is not versioned. A missing file is a **build error**:
there is no fallback, because a silent default renders an empty chart and an empty chart
is indistinguishable from a quiet market.

## Invariants

Asserted in `tests/test_dashboard_invariants.py` — in the Python suite, because that is
where CI runs and a criterion nothing executes cannot fail:

- the route set is EXACTLY the six (equality, not containment)
- `next.config.ts` declares `output: "export"`
- no source file mentions `svc.cluster.local` (checked after asserting there is something
  to scan, so it cannot pass vacuously)
- the loader has no `readJsonOr`-style fallback
- the freshness verdict is COMPUTED from published inputs, never read from the manifest
- the unit guard compares the whole `(unit, basis, scale)` tuple — keyed on `unit` alone it
  would pass the SELIC daily rate against monthly IPCA, since both are `percent`

The `dashboard` job in `.github/workflows/tests.yml` covers the one thing source cannot
answer: that the app still compiles and exports.

## Deploy

Static export, so any host serves it. See `docs/DEPLOY.md`; the hostname is a deploy-time
step and has not been executed.
