"""Build a release: read a source, run the gates, write the artefact.

Two abstractions and only two — a READER (``--from``) and a SINK (``--to``). That is what
makes the whole path exercisable offline: the CI runs ``file://`` end to end, and the
``gh://`` sink is a thin shell over an API call.

Identity is INPUT, not deduction. ``release_id`` and ``supersedes`` are both functions of
remote state (what has already been published), so an offline build cannot derive them.
The ``gh://`` sink fills them from ``latest.json`` before publishing; ``file://`` requires
them on the command line.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ofl.platform.logging import get_logger
from ofl.release import contracts as contracts_mod
from ofl.release.gates import (
    GateResult,
    gate_class,
    gate_contracts,
    gate_grain,
    gate_key_drift,
    gate_license,
    gate_release_id_format,
    gate_required,
)

if TYPE_CHECKING:
    import polars as pl

log = get_logger(__name__)

#: Statuses a table can carry in the manifest. Absence used to be one signal for four
#: different causes; a consumer could not tell an empty table from a withheld one.
TABLE_STATUSES = (
    "published",
    "empty",
    "withheld_gate",
    "withheld_license",
    "skipped_upstream",
    "removed",
)


class ReleaseError(RuntimeError):
    """A gate bit, or the build was asked for something it must refuse."""

    def __init__(self, message: str, *, gate: str | None = None, table: str | None = None):
        super().__init__(message)
        self.gate = gate
        self.table = table


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_source(source: str | Path, allowlist: Sequence[str]) -> dict[str, "pl.DataFrame"]:
    """Read every table in ``source``, refusing anything outside ``allowlist``.

    The reader is an allowlist of TABLES, not only a row filter, and it ABORTS on an
    unknown table rather than skipping it. The only ``.duckdb`` producer in this repo
    copies every mart out of MinIO with no filter at all, so "ignore what I do not
    recognise" is the shape a leak would take.

    CSV is accepted alongside parquet so the test corpus can be reviewed in a diff — a
    fixture nobody can read is a fixture nobody can check.
    """
    import polars as pl

    src = Path(source)
    if not src.is_dir():
        raise ReleaseError(f"source {src} is not a directory")

    allowed = set(allowlist)
    tables: dict[str, pl.DataFrame] = {}
    for path in sorted(list(src.glob("*.parquet")) + list(src.glob("*.csv"))):
        name = path.stem
        if name not in allowed:
            raise ReleaseError(
                f"table {name!r} is present in the source but not publishable "
                f"(allowlist: {sorted(allowed)}). Refusing to ignore it silently.",
                gate="reader_allowlist",
                table=name,
            )
        tables[name] = (
            pl.read_parquet(path) if path.suffix == ".parquet" else pl.read_csv(path, try_parse_dates=True)
        )
    return tables


def _run_gates(
    tables: dict[str, "pl.DataFrame"],
    contracts: dict[str, contracts_mod.TableContract],
    *,
    release_class: str,
    previous: dict[str, dict[str, Any]] | None,
) -> tuple[list[GateResult], dict[str, dict[str, Any]]]:
    results: list[GateResult] = []
    per_table: dict[str, dict[str, Any]] = {}

    for name, df in sorted(tables.items()):
        contract = contracts.get(name)
        pk = contract.primary_key if contract else []
        drift = _key_drift(name, df, pk, previous)
        checks = [
            gate_contracts(name, df, contract),
            gate_grain(name, df, pk),
            gate_required(name, df, contract),
            gate_class(name, df, release_class),
            gate_license(name, df),
            gate_key_drift(name, drift["keys_removed"], drift["rows_previous"]),
        ]
        results.extend(checks)
        per_table[name] = {"gates": checks, **drift}
    return results, per_table


def _key_drift(
    name: str,
    df: "pl.DataFrame",
    primary_key: Sequence[str],
    previous: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    """Compare this build's key set against the previous release's, when there is one."""
    prev = (previous or {}).get(name) or {}
    prev_keys = set(map(tuple, prev.get("keys", [])))
    if not primary_key or not all(k in df.columns for k in primary_key):
        return {"keys_added": 0, "keys_removed": 0, "rows_previous": len(prev_keys)}
    now_keys = set(map(tuple, df.select(list(primary_key)).rows()))
    return {
        "keys_added": len(now_keys - prev_keys),
        "keys_removed": len(prev_keys - now_keys),
        "rows_previous": len(prev_keys),
    }


def build_release(
    tables: dict[str, "pl.DataFrame"],
    contracts: dict[str, contracts_mod.TableContract],
    *,
    release_id: str,
    release_class: str,
    out_dir: str | Path,
    base_url: str = "",
    supersedes: str | None = None,
    upstream_commit: str = "",
    generated_at: dt.datetime | None = None,
    previous: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Write a complete release directory and return its manifest.

    Raises :class:`ReleaseError` naming the gate that bit. Nothing is written when a
    blocking gate fails — a half-written release directory is indistinguishable from a
    successful one to anything that only looks for files.
    """
    import polars as pl

    id_gate = gate_release_id_format(release_id)
    if not id_gate.ok:
        raise ReleaseError(id_gate.detail, gate=id_gate.name)

    if release_class not in ("production", "fixture"):
        raise ReleaseError(f"unknown release_class {release_class!r}", gate="release_class")
    if release_class == "production" and supersedes is None:
        raise ReleaseError(
            "a production release must state what it supersedes (use --supersedes none "
            "for the first one)",
            gate="supersedes",
        )

    gate_results, per_table = _run_gates(
        tables, contracts, release_class=release_class, previous=previous
    )
    failed = [g for g in gate_results if not g.ok]
    if failed:
        first = failed[0]
        raise ReleaseError(
            f"gate {first.name!r} failed for table {first.table!r}: {first.detail}",
            gate=first.name,
            table=first.table,
        )

    out = Path(out_dir)
    (out / "parquet").mkdir(parents=True, exist_ok=True)
    (out / "contracts").mkdir(parents=True, exist_ok=True)

    now = generated_at or dt.datetime.now(dt.timezone.utc)
    data_assets: list[dict[str, Any]] = []

    for name, df in sorted(tables.items()):
        rel = f"parquet/{name}.parquet"
        path = out / rel
        df.write_parquet(path, compression="zstd")
        contract = contracts[name]
        contracts_mod.dump_contract(contract, out / "contracts")
        drift = per_table[name]
        data_assets.append(
            {
                "name": name,
                "status": "empty" if df.height == 0 else "published",
                "reason": None,
                "primary_key": contract.primary_key,
                "contract_version": contract.contract_version,
                "contract_sha256": contract.sha256(),
                "rows": df.height,
                "path": rel,
                "sha256": _sha256(path),
                "keys_added": drift["keys_added"],
                "keys_removed": drift["keys_removed"],
                "revised_rows_vs_previous": None,
                "first_changed_date": None,
            }
        )

    # Tables that have a contract but did not arrive are DECLARED, not omitted.
    for name, contract in sorted(contracts.items()):
        if name in tables:
            continue
        data_assets.append(
            {
                "name": name,
                "status": "skipped_upstream",
                "reason": "no table of this name in the source",
                "primary_key": contract.primary_key,
                "contract_version": contract.contract_version,
                "contract_sha256": contract.sha256(),
                "rows": None,
                "path": None,
                "sha256": None,
            }
        )

    manifest: dict[str, Any] = {
        "release_id": release_id,
        "release_class": release_class,
        # A fixture release exists to exercise the pipeline, never to be distributed.
        # The `gh://` sink refuses this by construction rather than by remembering to.
        "publishable": release_class == "production",
        "immutable": True,
        "supersedes": supersedes,
        "base_url": base_url,
        "generated_at": now.isoformat(),
        "upstream_commit": upstream_commit,
        "duckdb_version": _duckdb_version(),
        "gates": [
            {"name": g.name, "status": g.status, "table": g.table, "detail": g.detail}
            for g in gate_results
        ],
        "data_assets": data_assets,
        "meta_assets": [],
        "series": _series_block(tables),
    }

    (out / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    meta = []
    for rel in ["manifest.json", *(f"contracts/{n}.contract.json" for n in sorted(contracts))]:
        p = out / rel
        if p.exists():
            meta.append({"path": rel, "sha256": _sha256(p)})
    manifest["meta_assets"] = meta

    # `meta_assets` carries checksums only, and no data_class. The golden CSVs and the
    # manifest itself are RESULTS of the pipeline, not observations of the world: they
    # have no provenance to declare, and asking them for one would make a fixture-derived
    # golden fail the very release that publishes it.
    (out / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    _write_checksums(out)

    log.info(
        "release_built",
        release_id=release_id,
        release_class=release_class,
        tables=len(tables),
        out=str(out),
    )
    return manifest


def _series_block(tables: dict[str, "pl.DataFrame"] | None = None) -> list[dict[str, Any]]:
    """Freshness inputs at the grain of the SERIES, never the table.

    ``fact_observation`` is the union of series from daily to quarterly. One
    ``last_observation_date`` for the whole table reads green whenever any daily series is
    current, even with a whole domain three months stale.

    The verdict is never published — only ``last_observation_date`` and
    ``freshness_budget_hours``. A manifest from three days ago still answers RED correctly
    without being republished; one that declares itself "ok" lies on exactly the day the
    producer dies.
    """
    import polars as pl

    from ofl.registry import load_registry

    default_hours = {"daily": 36, "weekly": 240, "monthly": 800, "quarterly": 2400, "annual": 9000}

    # MEASURED from the data that is actually being published, per series. This was a
    # `None` placeholder until the site was built and every freshness chip read "sem
    # dado" — a chip that can never be green is the mirror of a manifest that declares
    # itself "ok": both are decorations that cannot report the state they exist to report.
    last_seen: dict[str, str] = {}
    obs = (tables or {}).get("fact_observation")
    if obs is not None and {"series_id", "date"} <= set(obs.columns):
        grouped = obs.group_by("series_id").agg(pl.col("date").max().alias("last"))
        last_seen = {r["series_id"]: str(r["last"]) for r in grouped.iter_rows(named=True)}

    out = []
    for s in load_registry().active():
        out.append(
            {
                "series_id": s.key,
                "unit": s.unit,
                "basis": s.basis,
                "scale": s.scale,
                "day_count": s.day_count,
                "horizon": s.horizon,
                "frequency": s.frequency,
                "provider": s.handler,
                "freshness_budget_hours": default_hours.get(s.frequency, 800),
                "last_observation_date": last_seen.get(s.key),
            }
        )
    return out


def _duckdb_version() -> str:
    try:
        import duckdb

        return str(duckdb.__version__)
    except Exception:  # noqa: BLE001 - the mirror is optional; its version is metadata
        return ""


def _write_checksums(out: Path) -> None:
    lines = []
    for path in sorted(out.rglob("*")):
        if path.is_file() and path.name != "checksums.sha256":
            lines.append(f"{_sha256(path)}  {path.relative_to(out).as_posix()}")
    (out / "checksums.sha256").write_text("\n".join(lines) + "\n", encoding="utf-8")
