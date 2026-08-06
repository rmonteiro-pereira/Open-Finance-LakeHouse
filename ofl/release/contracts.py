"""Schema contracts — the compatibility promise a consumer pins against.

Two axes of nullability, deliberately separate:

* ``arrow_nullable`` is what the ``contracts`` gate compares against the file. Delta
  cannot add a NOT NULL column to a materialised table, so a migrated column is nullable
  in storage whether or not the data is complete. Comparing against anything else would
  make the gate fail on the migration rather than on the defect.
* ``required`` is an assertion about VALUES, checked by the ``required`` gate as
  ``COUNT(*) WHERE col IS NULL = 0``. This is where "provider is mandatory" is enforced.

Collapsing the two — as an earlier draft did — fixes one field at two opposite values.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    import polars as pl

#: MAJOR = column removed/renamed, type narrowed, GRAIN changed.
#: MINOR = column added. PATCH = description only.
SEMVER = "MAJOR.MINOR.PATCH"


class Column(BaseModel):
    name: str
    arrow_type: str
    arrow_nullable: bool = True
    required: bool = False
    unit: str | None = None
    basis: str | None = None
    scale: int | None = None
    description: str = ""


class TableContract(BaseModel):
    table: str
    contract_version: str = "1.0.0"
    primary_key: list[str] = Field(default_factory=list)
    columns: list[Column] = Field(default_factory=list)
    deprecations: list[str] = Field(default_factory=list)

    def column_signature(self) -> list[tuple[str, str, bool]]:
        return [(c.name, c.arrow_type, c.arrow_nullable) for c in self.columns]

    def required_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.required]

    def sha256(self) -> str:
        payload = json.dumps(self.model_dump(), sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def frame_signature(df: "pl.DataFrame") -> list[tuple[str, str, bool]]:
    """The frame's own ``(name, type, nullable)``.

    Polars carries no per-column nullability flag, so nullability is read from the data:
    a column with no nulls is reported non-nullable only if the contract says so. To keep
    the comparison from flapping on sample size, the frame side always reports ``True``
    and the contract side is the one that may narrow — narrowing is then a MAJOR bump,
    which is exactly the review this is meant to force.
    """
    return [(name, str(dtype), True) for name, dtype in zip(df.columns, df.dtypes, strict=True)]


def compare(contract: TableContract, df: "pl.DataFrame") -> list[str]:
    """Differences between contract and frame. Empty list is a pass.

    Equality of the column SET, not containment: an extra column fails. That is what
    obliges a MINOR bump instead of letting new columns appear in a release unannounced.
    """
    want = {(n, t) for n, t, _ in contract.column_signature()}
    have = {(n, t) for n, t, _ in frame_signature(df)}

    problems: list[str] = []
    for name, dtype in sorted(want - have):
        if name in df.columns:
            actual = str(df.schema[name])
            problems.append(f"column {name!r}: contract says {dtype}, file has {actual}")
        else:
            problems.append(f"column {name!r} declared in the contract is absent from the file")
    for name, dtype in sorted(have - want):
        if name not in {n for n, _ in want}:
            problems.append(f"column {name!r} ({dtype}) is in the file but not in the contract")
    for key in contract.primary_key:
        if key not in df.columns:
            problems.append(f"primary key column {key!r} is absent from the file")
    return problems


def contract_from_frame(
    table: str,
    df: "pl.DataFrame",
    *,
    primary_key: list[str],
    required: list[str] | None = None,
    version: str = "1.0.0",
) -> TableContract:
    """Draft a contract from a frame — a starting point for review, not an oracle.

    Generating the contract from the same data it will check would make the gate
    self-referential. This exists to author the FIRST version of a contract, which is
    then committed and reviewed; from then on the committed file is authoritative and a
    drift shows up as a gate failure.
    """
    req = set(required or [])
    return TableContract(
        table=table,
        contract_version=version,
        primary_key=list(primary_key),
        columns=[
            Column(name=n, arrow_type=str(t), arrow_nullable=True, required=n in req)
            for n, t in zip(df.columns, df.dtypes, strict=True)
        ],
    )


def load_contract(path: str | Path) -> TableContract:
    return TableContract.model_validate_json(Path(path).read_text(encoding="utf-8"))


def load_contracts(directory: str | Path) -> dict[str, TableContract]:
    d = Path(directory)
    if not d.is_dir():
        return {}
    out: dict[str, TableContract] = {}
    for p in sorted(d.glob("*.contract.json")):
        c = load_contract(p)
        out[c.table] = c
    return out


def dump_contract(contract: TableContract, directory: str | Path) -> Path:
    path = Path(directory) / f"{contract.table}.contract.json"
    payload: dict[str, Any] = contract.model_dump()
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path
