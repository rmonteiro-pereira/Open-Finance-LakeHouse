from datetime import date

from ofl.ingestion.b3_cotahist import parse_cotahist

# One COTAHIST detail record (TIPREG=01), built field-by-field to the official
# 245-byte layout. Numeric fields are zero-filled; text fields are space-padded.
_FIELDS = [
    ("01", 2),            # TIPREG
    ("20240305", 8),      # DATA
    ("02", 2),            # CODBDI (round lot)
    ("PETR4", 12),        # CODNEG
    ("010", 3),           # TPMERC (cash)
    ("PETROBRAS", 12),    # NOMRES
    ("PN", 10),           # ESPECI
    ("", 3),              # PRAZOT
    ("R$", 4),            # MODREF
    ("0000000003800", 13),  # PREABE 38.00
    ("0000000003900", 13),  # PREMAX 39.00
    ("0000000003750", 13),  # PREMIN 37.50
    ("0000000003850", 13),  # PREMED
    ("0000000003880", 13),  # PREULT 38.80
    ("0000000003879", 13),  # PREOFC
    ("0000000003881", 13),  # PREOFV
    ("01234", 5),           # TOTNEG 1234
    ("000000000005000000", 18),  # QUATOT 5,000,000
    ("000000019400000000", 18),  # VOLTOT 194,000,000.00
    ("0000000000000", 13),  # PREEXE
    ("0", 1),               # INDOPC
    ("99991231", 8),        # DATVEN
    ("0000001", 7),         # FATCOT 1
    ("0000000000000", 13),  # PTOEXE
    ("BRPETRACNPR6", 12),   # CODISI
    ("100", 3),             # DISMES
]


def _record() -> str:
    line = "".join(v.rjust(w, "0") if v.isdigit() else v.ljust(w) for v, w in _FIELDS)
    assert len(line) == 245, len(line)
    return line


def test_parse_round_lot_record():
    df = parse_cotahist((_record() + "\r\n").encode("latin-1"))
    assert df.height == 1
    r = df.row(0, named=True)
    assert r["symbol"] == "PETR4"
    assert r["codbdi"] == "02"
    assert r["tpmerc"] == "010"
    assert r["date"] == date(2024, 3, 5)
    assert (r["open"], r["high"], r["low"], r["close"]) == (38.00, 39.00, 37.50, 38.80)
    assert r["volume"] == 5_000_000
    assert r["volume_brl"] == 194_000_000.00
    assert r["trades"] == 1234
    assert r["fatcot"] == 1


def test_header_and_trailer_skipped():
    header = "00" + " " * 243
    trailer = "99" + " " * 243
    df = parse_cotahist((header + "\r\n" + trailer + "\r\n").encode("latin-1"))
    assert df.height == 0
