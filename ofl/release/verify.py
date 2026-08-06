"""Verify a release directory — a separate process from the one that built it.

Whoever checks must not be whoever produced. The builder knows what it intended; the
verifier is handed a directory and asked whether that directory is publishable. It reads
the files that are actually there, not the frames the builder held in memory.

Two structural rules, both learned the hard way:

* **Allowlist, not denylist.** A table with no ``provider`` column is rejected BEFORE any
  value is examined. Both licence mechanisms are predicates over a column that may not
  exist, and a predicate over an absent column is satisfied by its absence — which is how
  a derived table born without provenance passes every check meant to stop it.
* **Every file, not every parquet.** The verifier enumerates the whole directory and
  matches each extension to a reader. An earlier draft scoped itself to "each parquet",
  which left the DuckDB mirror, the golden CSVs and the catalogue outside the check it
  claimed to perform.

Exit codes are distinct on purpose: ``2`` is a usage error (no such directory, malformed
manifest), ``3`` is a gate that bit. A negative test that only asserts "non-zero" passes
when the path was simply wrong.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ofl.platform.logging import get_logger
from ofl.release.contracts import load_contracts
from ofl.release.gates import CLASS_POLICY, gate_class, gate_contracts, gate_grain, gate_license, gate_required

log = get_logger(__name__)

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_GATE = 3

DATA_SUFFIXES = {".parquet", ".csv", ".duckdb"}


@dataclass
class VerifyReport:
    release_id: str = ""
    release_class: str = ""
    expect_class: str = ""
    ok: bool = True
    failed_gate: str | None = None
    table: str | None = None
    detail: str = ""
    checked_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "release_id": self.release_id,
            "release_class": self.release_class,
            "expect_class": self.expect_class,
            "ok": self.ok,
            "failed_gate": self.failed_gate,
            "table": self.table,
            "detail": self.detail,
            "checked_files": self.checked_files,
        }

    @property
    def exit_code(self) -> int:
        return EXIT_OK if self.ok else EXIT_GATE


class UsageError(RuntimeError):
    """The directory or the manifest is not something that can be verified at all."""


def _read_data_file(path: Path):
    import polars as pl

    if path.suffix == ".parquet":
        return {path.stem: pl.read_parquet(path)}
    if path.suffix == ".csv":
        return {path.stem: pl.read_csv(path, try_parse_dates=True)}
    if path.suffix == ".duckdb":
        import duckdb

        con = duckdb.connect(str(path), read_only=True)
        names = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        return {n: con.execute(f'SELECT * FROM "{n}"').pl() for n in names}
    return {}


def verify_release(directory: str | Path, *, expect_class: str = "production") -> VerifyReport:
    """Check a built release. ``expect_class`` is the VERIFIER's question, not the file's.

    A verify with no qualification asks "is this publishable in production?", so the
    default is ``production`` regardless of what the manifest says about itself. The
    manifest's own consistency (``release_class`` vs ``publishable``) is checked
    separately and always — otherwise a fixture release would pass a bare ``verify`` just
    by declaring itself a fixture, and the gate would only ever confirm the label.
    """
    d = Path(directory)
    if not d.is_dir():
        raise UsageError(f"{d} is not a directory")
    manifest_path = d / "manifest.json"
    if not manifest_path.is_file():
        raise UsageError(f"{manifest_path} not found")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise UsageError(f"manifest is not valid JSON: {exc}") from exc

    if expect_class not in CLASS_POLICY:
        raise UsageError(f"unknown expect_class {expect_class!r}")

    report = VerifyReport(
        release_id=manifest.get("release_id", ""),
        release_class=manifest.get("release_class", ""),
        expect_class=expect_class,
    )

    def fail(gate: str, detail: str, table: str | None = None) -> VerifyReport:
        report.ok = False
        report.failed_gate = gate
        report.detail = detail
        report.table = table
        return report

    # Internal consistency first: a fixture that claims to be publishable is a broken
    # artefact regardless of what anyone asked.
    declared = manifest.get("release_class")
    if declared not in CLASS_POLICY:
        return fail("class", f"manifest declares unknown release_class {declared!r}")
    if manifest.get("publishable") is not (declared == "production"):
        return fail(
            "class",
            f"manifest says release_class={declared!r} but publishable={manifest.get('publishable')!r}",
        )

    contracts = load_contracts(d / "contracts")

    for path in sorted(p for p in d.rglob("*") if p.is_file()):
        if path.suffix not in DATA_SUFFIXES:
            continue
        report.checked_files.append(path.relative_to(d).as_posix())
        for table, df in _read_data_file(path).items():
            # Structural allowlist BEFORE any value is looked at.
            if "provider" not in df.columns and "providers" not in df.columns:
                return fail("license", f"{path.name}: no provenance column", table)
            if "data_class" not in df.columns:
                return fail("class", f"{path.name}: no `data_class` column", table)

            contract = contracts.get(table)
            for gate in (
                gate_contracts(table, df, contract),
                gate_grain(table, df, contract.primary_key if contract else []),
                gate_required(table, df, contract),
                gate_class(table, df, expect_class),
                gate_license(table, df),
            ):
                if not gate.ok:
                    return fail(gate.name, gate.detail, gate.table)

    if not report.checked_files:
        return fail("empty_release", "no data file found in the release directory")

    log.info("release_verified", release_id=report.release_id, files=len(report.checked_files))
    return report


def write_report(report: VerifyReport, directory: str | Path) -> Path:
    path = Path(directory) / "verify_report.json"
    path.write_text(json.dumps(report.to_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path
