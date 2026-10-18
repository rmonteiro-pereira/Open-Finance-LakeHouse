"""Portable dataset verifier: Python 3.11+ standard library, no network or database.

This file is included as reproduce.py in an OFL dataset bundle. It independently
recomputes descriptive transformations from the captured input observations.
"""

import argparse
import csv
import hashlib
import io
import json
from decimal import Decimal, localcontext
from pathlib import Path

MAX_BYTES = 64 * 1024**2


def replay(folder):
    receipt = json.loads((folder / "receipt.json").read_text())
    if receipt.get("version") != 1:
        raise ValueError("unsupported receipt version")
    payloads = {}
    for name in ("inputs.json", "manifest.json", "dataset.csv"):
        path = folder / name
        if path.stat().st_size > MAX_BYTES:
            raise ValueError("bundle member exceeds size bound")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != receipt["sha256"][name]:
            raise ValueError(f"checksum mismatch: {name}")
        payloads[name] = raw
    manifest = json.loads(payloads["manifest.json"])
    sources = json.loads(payloads["inputs.json"])
    spec = manifest["specification"]
    if manifest["method"] != "ofl/descriptive-dataset/1":
        raise ValueError("unsupported transformation method")
    if not 1 <= len(spec["series"]) <= 8 or len(sources) != len(spec["series"]):
        raise ValueError("input/recipe series mismatch")
    columns, date_sets = {}, []
    with localcontext() as context:
        context.prec = 50
        for request, source in zip(spec["series"], sources, strict=True):
            if (
                source["snapshot_id"] != spec["snapshot_id"]
                or source["series"]["key"] != request["series_id"]
            ):
                raise ValueError("input identity differs from pinned recipe")
            if source.get("has_more") or source["offset"] != 0:
                raise ValueError("incomplete captured source query")
            rows = source["rows"]
            dates = [str(row["date"]) for row in rows]
            if dates != sorted(set(dates)):
                raise ValueError("input dates must be unique and ordered")
            if any(
                (spec["date_from"] and day < spec["date_from"])
                or (spec["date_to"] and day > spec["date_to"])
                for day in dates
            ):
                raise ValueError("input date outside recipe")
            values = [None if row["value"] is None else Decimal(row["value"]) for row in rows]
            if any(value is not None and not value.is_finite() for value in values):
                raise ValueError("non-finite input")
            base = next((value for value in values if value is not None), None)
            transformed = []
            operation = request["operation"]
            for index, value in enumerate(values):
                previous = values[index - request["lag"]] if index >= request["lag"] else None
                if operation == "level":
                    result = value
                elif operation == "difference":
                    result = None if value is None or previous is None else value - previous
                elif operation == "percent_change":
                    result = (
                        None
                        if value is None or previous in (None, 0)
                        else (value - previous) / previous * 100
                    )
                elif operation == "index_100":
                    result = None if value is None or base in (None, 0) else value / base * 100
                elif operation == "rolling_mean":
                    window = values[max(0, index - request["window"] + 1) : index + 1]
                    result = (
                        None
                        if len(window) != request["window"] or any(v is None for v in window)
                        else sum(window) / request["window"]
                    )
                else:
                    raise ValueError("unsupported operation")
                transformed.append(None if result is None else str(result))
            name = request["series_id"] + "__" + operation
            if name in columns:
                raise ValueError("duplicate output column")
            columns[name] = dict(zip(dates, transformed, strict=True))
            date_sets.append(set(dates))
    if spec["alignment"] not in {"inner", "outer"}:
        raise ValueError("unsupported alignment")
    aligned = (
        set.intersection(*date_sets) if spec["alignment"] == "inner" else set.union(*date_sets)
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["date", *columns])
    for day in sorted(aligned):
        writer.writerow([day, *[values.get(day) for values in columns.values()]])
    raw = output.getvalue().encode()
    if (
        len(aligned) != manifest["rows"]
        or hashlib.sha256(raw).hexdigest() != manifest["csv_sha256"]
    ):
        raise ValueError("recomputed result differs from expected rows/checksum")
    if raw != payloads["dataset.csv"]:
        raise ValueError("recomputed bytes differ from bundled dataset")
    return raw


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", nargs="?", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    raw = replay(args.folder)
    target = args.output or args.folder / "dataset.reproduced.csv"
    with target.open("xb") as stream:
        stream.write(raw)
    print(
        json.dumps(
            {
                "status": "reproduced",
                "csv_sha256": hashlib.sha256(raw).hexdigest(),
                "qualification": (
                    "Captured observations reproduced offline; checksums do not "
                    "independently authenticate source truth or historical availability."
                ),
            }
        )
    )


if __name__ == "__main__":
    main()
