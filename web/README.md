# web: the public face of the lakehouse

A Next.js site that shows what the pipeline produced. It never talks to the cluster:
it reads the snapshot that `ofl publish` writes to a public bucket on the lakehouse's own store after each gold run
(`latest.json`, then the manifest, then the Parquet files it lists). When the cluster is
down or busy, the site keeps serving the last snapshot and says how old it is.

## Run it

```bash
bun install
SNAPSHOT_URL=https://<where the deployment serves the bucket> bun run dev
```

`SNAPSHOT_URL` is the only setting. For a local snapshot, run `ofl publish` with
`OFL_PUBLISH_DIR=<folder>` and serve that folder over HTTP.

## How it is put together

- `server/snapshot.ts` loads the manifest, the catalogue and the marts the page charts.
  The result is cached for ten minutes (`use cache`), so a new snapshot shows up without
  a deploy.
- `components/LineChart.tsx` is the only chart: SVG, a crosshair that reads every series
  at once, and the same values as a table underneath.
- Colours follow a palette checked for colour-blind separation in light and dark.

Licence tiers come from the snapshot, not from this code: a mart marked `download: false`
is charted but not linked, and a withheld mart is listed with the reason.
