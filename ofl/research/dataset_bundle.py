"""Portable, bounded dataset exports; no original private lakehouse object paths."""

import hashlib
import io
import json
import zipfile
from importlib.resources import files

from .analytics import prepare_dataset

MAX_BYTES = 64 * 1024**2


def bundle(db, store, specification):
    sources = []
    prepared = prepare_dataset(db, store, specification, source_capture=sources)
    members = {
        "inputs.json": json.dumps(sources, ensure_ascii=False, default=str).encode(),
        "manifest.json": json.dumps(prepared["manifest"], ensure_ascii=False, indent=2).encode(),
        "dataset.csv": prepared["csv"].encode(),
        "reproduce.py": files("ofl.research").joinpath("dataset_replay.py").read_bytes(),
        "README.txt": (
            "OFL reproducible descriptive dataset\n\n"
            "Extract this archive into a new directory. Inspect reproduce.py and run:\n"
            "  python3 reproduce.py\n"
            "Requires Python 3.11+, standard library only. No network, database or model.\n"
            "The script verifies inputs and recomputes dataset.reproduced.csv.\n"
            "Existing output files are never overwritten.\n"
            "Retain receipt.json from the authenticated export.\n"
            "Hashes detect changes; they are not signatures.\n"
            "Observation dates are not publication dates. No imputation or hidden warm-up.\n"
            "This is not a backtest or an independent verification of source correctness.\n"
        ).encode(),
    }
    if sum(map(len, members.values())) > MAX_BYTES:
        raise ValueError("reproduction bundle exceeds 64 MiB; narrow dates or series")
    members["receipt.json"] = json.dumps(
        {
            "version": 1,
            "sha256": {name: hashlib.sha256(raw).hexdigest() for name, raw in members.items()},
        },
        indent=2,
    ).encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, raw in members.items():
            # Fixed member names/timestamps; identical pinned inputs yield identical archives.
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            archive.writestr(info, raw)
    return output.getvalue()
