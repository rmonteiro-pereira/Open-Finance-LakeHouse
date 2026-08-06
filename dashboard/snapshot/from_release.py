#!/usr/bin/env python3
"""Build the site's data from a PUBLISHED RELEASE — the only supported source.

This replaces both producers that came before it, and the replacement is the point:

* ``gen_synthetic.py`` generated a realistic mock. Shipping it as the default meant the
  site's out-of-the-box state was invented numbers wearing the same chrome as real ones.
* ``export.py`` read Delta from MinIO. That made the reader surface a dependent of the
  homelab: a surface that needs the cluster is a surface that dies with the cluster.

The site now consumes the same artefact any external consumer does, which is what turns
the boundary from an intention into something that breaks loudly when violated. It is
also the only way the dashboard can serve as the first real consumer of the release —
proving the interface by use rather than by assertion.

Missing data is a BUILD ERROR. A silent fallback renders an empty chart, and an empty
chart is indistinguishable from a quiet market.

    python snapshot/from_release.py --release ../out
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

OUT_DIR = Path(__file__).resolve().parent.parent / "public" / "data"

#: Manifest table -> the JSON file the site loads. Anything not listed is not shipped to
#: the browser, so widening the site's data surface is an explicit edit here.
TABLES = {
    "fact_observation": "fact_observation",
    "fact_tesouro_direto": "fact_tesouro_direto",
    "mart_series_percentile": "mart_series_percentile",
}


class ReleaseUnusable(RuntimeError):
    """The release cannot feed a build, and the build must not proceed."""


def _read_table(release: Path, asset: dict[str, Any]):
    import polars as pl

    rel = asset.get("path")
    if not rel:
        raise ReleaseUnusable(
            f"table {asset['name']!r} has status {asset.get('status')!r} and no file: "
            f"{asset.get('reason') or 'no reason recorded'}"
        )
    path = release / rel
    if not path.is_file():
        raise ReleaseUnusable(f"{path} is named in the manifest but absent from the release")
    return pl.read_parquet(path) if path.suffix == ".parquet" else pl.read_csv(path, try_parse_dates=True)


def build(release_dir: str | Path, out_dir: Path = OUT_DIR) -> dict[str, Any]:
    manifest_path = Path(release_dir) / "manifest.json"
    if not manifest_path.is_file():
        raise ReleaseUnusable(f"{manifest_path} not found — point --release at a built release")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    failed = [g for g in manifest.get("gates", []) if g.get("status") != "pass"]
    if failed:
        raise ReleaseUnusable(f"the release has failing gates: {[g['name'] for g in failed]}")

    out_dir.mkdir(parents=True, exist_ok=True)
    assets = {a["name"]: a for a in manifest.get("data_assets", [])}
    written = []
    for table, filename in TABLES.items():
        asset = assets.get(table)
        if asset is None:
            raise ReleaseUnusable(f"the release does not declare table {table!r}")
        df = _read_table(Path(release_dir), asset)
        (out_dir / f"{filename}.json").write_text(
            json.dumps(json.loads(df.write_json()), ensure_ascii=False), encoding="utf-8"
        )
        written.append(filename)

    # `generated_from` is what the build gate reads. It says "release", and it says WHICH
    # release, so a stale page can be traced to the artefact that produced it.
    meta = {
        "generated_from": "release",
        "release_id": manifest["release_id"],
        "release_class": manifest["release_class"],
        "generated_at": manifest["generated_at"],
        "series": manifest.get("series", []),
        "gates": manifest.get("gates", []),
        "tables": [
            {k: assets[t].get(k) for k in ("name", "status", "rows", "primary_key", "sha256")}
            for t in TABLES
            if t in assets
        ],
    }
    (out_dir / "_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"written": written, "release_id": manifest["release_id"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True, help="a directory built by `ofl release build`")
    args = parser.parse_args(argv)
    try:
        result = build(args.release)
    except ReleaseUnusable as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
