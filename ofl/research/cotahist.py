"""All COTAHIST fields, exact decimals, bounded ZIP parsing, and market identity.

Specification: B3 SeriesHistoricas_Layout.pdf, revision 02 (2020-10-05).
Positions below are zero-based, lengths exclude CR/LF. No source downloads.
"""

import hashlib
import re
import zipfile
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath

import pyarrow as pa
import pyarrow.parquet as pq

from .db import digest

VERSION = "cotahist-v2"
MARKETS = {
    "010": "cash",
    "012": "call_exercise",
    "013": "put_exercise",
    "017": "auction",
    "020": "fractional",
    # Observed as TPMERC=021/CODBDI=93 in official 2026 daily files. The
    # published layout does not name it: preserve its code without guessing a label.
    "021": "unclassified_021",
    "030": "forward",
    "050": "future_retained",
    "060": "future_continuous",
    "070": "call",
    "080": "put",
}
FUTURES_MARKETS = {"050", "060"}
FUTURES_BDI = {"70", "71"}
TEXT_FIELDS = {
    "codbdi": (10, 2),
    "symbol": (12, 12),
    "tpmerc": (24, 3),
    "issuer_name": (27, 12),
    "specification": (39, 10),
    "forward_term": (49, 3),
    "currency": (52, 4),
    "correction_indicator": (201, 1),
    "expiry_raw": (202, 8),
    "isin": (230, 12),
    "distribution": (242, 3),
}
MONEY_FIELDS = {
    "open": (56, 13),
    "high": (69, 13),
    "low": (82, 13),
    "average": (95, 13),
    "close": (108, 13),
    "bid": (121, 13),
    "ask": (134, 13),
    "turnover": (170, 18),
    "exercise_price": (188, 13),
}
INTEGER_FIELDS = {"trades": (147, 5), "quantity": (152, 18), "quotation_factor": (210, 7)}
IDENTITY = [
    "date",
    "symbol",
    "tpmerc",
    "codbdi",
    "specification",
    "isin",
    "distribution",
    "forward_term",
    "expiry_raw",
    "correction_indicator",
    "currency",
    "quotation_factor",
    "exercise_price_scaled",
    "exercise_points_scaled",
]
SCHEMA = pa.schema(
    [
        pa.field("record_id", pa.string()),
        pa.field("date", pa.date32()),
        *[pa.field(k, pa.string()) for k in TEXT_FIELDS],
        pa.field("expiry", pa.date32()),
        *[
            field
            for k in MONEY_FIELDS
            for field in (pa.field(k + "_scaled", pa.int64()), pa.field(k, pa.decimal128(20, 2)))
        ],
        *[pa.field(k, pa.int64()) for k in INTEGER_FIELDS],
        pa.field("exercise_points_scaled", pa.int64()),
        pa.field("exercise_points", pa.decimal128(20, 6)),
        pa.field("source_hash", pa.string()),
        pa.field("source_line", pa.int64()),
        pa.field("row_hash", pa.string()),
    ]
)


def _date(value):
    return datetime.strptime(value, "%Y%m%d").date()


def parse_record(raw: bytes, source_hash: str, line: int):
    if len(raw) != 245:
        raise ValueError(f"line {line}: expected 245 bytes, got {len(raw)}")
    text = raw.decode("latin-1")
    row = {k: text[start : start + length].strip() for k, (start, length) in TEXT_FIELDS.items()}
    row["date"] = _date(text[2:10])
    row["expiry"] = (
        None if row["expiry_raw"] in {"00000000", "99991231", ""} else _date(row["expiry_raw"])
    )
    for k, (start, length) in MONEY_FIELDS.items():
        value = text[start : start + length]
        if not value.isdigit():
            raise ValueError(f"line {line}: invalid {k}")
        row[k + "_scaled"] = int(value)
        row[k] = Decimal(value) / 100
    for k, (start, length) in INTEGER_FIELDS.items():
        value = text[start : start + length]
        if not value.isdigit():
            raise ValueError(f"line {line}: invalid {k}")
        row[k] = int(value)
    value = text[217:230]
    if not value.isdigit():
        raise ValueError(f"line {line}: invalid exercise_points")
    row["exercise_points_scaled"] = int(value)
    row["exercise_points"] = Decimal(value) / 1_000_000
    if not row["symbol"] or not row["currency"]:
        raise ValueError(f"line {line}: missing identity")
    row["record_id"] = digest([VERSION, *[row[k] for k in IDENTITY]])
    row["row_hash"] = hashlib.sha256(raw).hexdigest()
    row["source_hash"], row["source_line"] = source_hash, line
    return row


@contextmanager
def records(path: Path, max_uncompressed_bytes: int):
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if len(members) != 1:
                raise ValueError("COTAHIST ZIP must contain exactly one TXT member")
            entry = members[0]
            member = PurePosixPath(entry.filename.replace("\\", "/"))
            if member.is_absolute() or ".." in member.parts or entry.is_dir():
                raise ValueError("unsafe ZIP member")
            if entry.flag_bits & 1 or entry.file_size > max_uncompressed_bytes:
                raise ValueError("encrypted or oversized ZIP member")
            with archive.open(entry) as stream:
                yield stream
    else:
        if path.stat().st_size > max_uncompressed_bytes:
            raise ValueError("oversized TXT")
        with path.open("rb") as stream:
            yield stream


def stage(
    path: Path,
    output: Path,
    source_hash: str,
    *,
    input_name: str | None = None,
    batch_rows=50_000,
    max_uncompressed_bytes=32 * 1024**3,
    heartbeat=lambda _: None,
):
    """Write record-01 rows to bounded Parquet parts; never publish partial files."""
    output.mkdir(parents=True, exist_ok=True)
    counts = {"records": 0, "accepted": 0, "excluded_futures": 0, "quarantined": 0}
    rows, part, byte_count = [], 0, 0
    dates = set()
    header = trailer = None
    with records(path, max_uncompressed_bytes) as stream:
        for line_no, line in enumerate(stream, 1):
            byte_count += len(line)
            if byte_count > max_uncompressed_bytes:
                raise ValueError("decompressed input exceeds limit")
            raw = line.rstrip(b"\r\n")
            if len(raw) != 245:
                raise ValueError(f"line {line_no}: invalid record length")
            record_type = raw[:2]
            if record_type == b"00" and line_no == 1:
                header = raw
                continue
            if record_type == b"99" and header and not trailer:
                trailer = raw
                continue
            if record_type != b"01" or header is None or trailer is not None:
                raise ValueError(f"line {line_no}: invalid record order/type")
            counts["records"] += 1
            row = parse_record(raw, source_hash, line_no)
            market_future, bdi_future = (
                row["tpmerc"] in FUTURES_MARKETS,
                row["codbdi"] in FUTURES_BDI,
            )
            if market_future != bdi_future:
                raise ValueError(f"line {line_no}: conflicting futures classification")
            if row["tpmerc"] not in MARKETS:
                raise ValueError(f"line {line_no}: unknown market {row['tpmerc']}")
            if row["tpmerc"] == "021" and row["codbdi"] != "93":
                raise ValueError(f"line {line_no}: unrecognized BDI for market 021")
            dates.add(row["date"].isoformat())
            if market_future:
                counts["excluded_futures"] += 1
                continue
            counts["accepted"] += 1
            rows.append(row)
            if len(rows) >= batch_rows:
                pq.write_table(
                    pa.Table.from_pylist(rows, schema=SCHEMA), output / f"{part:06d}.parquet"
                )
                rows, part = [], part + 1
                heartbeat(counts)
        if rows or part == 0:
            pq.write_table(
                pa.Table.from_pylist(rows, schema=SCHEMA), output / f"{part:06d}.parquet"
            )
    if header is None or trailer is None:
        raise ValueError("missing header/trailer")
    if header[2:31] != trailer[2:31] or int(trailer[31:42]) != line_no:
        raise ValueError("header/trailer identity or count mismatch")
    generated = _date(header[23:31].decode("ascii"))
    # The 13-byte header contains the year. Frequency comes from the supplied
    # official filename, never from upload time or the content-hash storage path.
    name = header[2:15].decode("ascii").strip()
    match = re.fullmatch(r"COTAHIST\.(\d{4})", name)
    if not match:
        raise ValueError("unrecognized COTAHIST header period")
    period = match[1]
    if input_name:
        supplied = re.fullmatch(r"COTAHIST_([AMD])(\d+)\.(ZIP|TXT)", Path(input_name).name.upper())
        if not supplied or len(supplied[2]) != {"A": 4, "M": 6, "D": 8}[supplied[1]]:
            raise ValueError("supply an official COTAHIST_A/M/D filename")
        period = supplied[2]
        if period[-4:] != match[1]:
            raise ValueError("filename/header year mismatch")
    frequency = {4: 1, 6: 2, 8: 3}[len(period)]
    if len(period) == 4:
        valid = lambda d: d[:4] == period
    elif len(period) == 6:
        # Monthly headers use MMAAAA; daily headers DDMMAAAA.
        valid = lambda d: d[:7] == f"{period[2:]}-{period[:2]}"
    else:
        day = datetime.strptime(period, "%d%m%Y").date().isoformat()
        valid = lambda d: d == day
    if any(not valid(d) for d in dates):
        raise ValueError("trading date outside declared coverage")
    # Validate the complete staged file before any Delta writes. Older official
    # archives can contain contradictory rows even when header/trailer checks pass.
    import tempfile

    import duckdb

    with tempfile.TemporaryDirectory(dir=output) as scratch, duckdb.connect() as query:
        query.execute("SET memory_limit='512MB'")
        query.execute("SET threads=2")
        query.execute("SET temp_directory=?", (scratch,))
        query.read_parquet(str(output / "*.parquet")).create_view("staged")
        conflict = query.execute("""SELECT record_id,min(date),min(symbol),min(tpmerc),
            min(source_line),max(source_line) FROM staged GROUP BY record_id
            HAVING count(DISTINCT row_hash)>1 ORDER BY min(source_line) LIMIT 1""").fetchone()
        if conflict:
            identity, day, symbol, market, first, last = conflict
            raise ValueError(
                f"conflicting quote identity {identity} ({day}, {symbol}, market {market}; "
                f"source lines {first} and {last})"
            )
    return {
        "counts": counts,
        "generated": generated,
        "frequency": frequency,
        "dates": sorted(dates),
        "period": period,
    }
