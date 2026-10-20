"""Offline contracts for exact values: COTAHIST parsing, quote bases, portfolios, tables."""

import zipfile
from datetime import date
from decimal import Decimal, localcontext

import pytest

pytest.importorskip("psycopg")

import pyarrow.parquet as pq

from ofl.research import cotahist, query
from ofl.research.analytics import compare_quotes, number, portfolio
from ofl.research.config import ResearchSettings
from ofl.research.query import per_unit
from ofl.research.review import normalize, observation_period
from tests.test_research import file_bytes, record, write_input


def exact(numerator, denominator, scale=1):
    """Division in the 50-digit context the analytics module documents."""
    with localcontext() as context:
        context.prec = 50
        return str(Decimal(numerator) / Decimal(denominator) * scale)


# Quotation basis


def test_per_unit_divides_by_the_quotation_factor_to_twelve_places():
    assert per_unit(Decimal("38.80"), 1) == Decimal("38.800000000000")
    assert per_unit(Decimal("38.80"), 1000) == Decimal("0.038800000000")
    assert per_unit(Decimal("1"), 3) == Decimal("0.333333333333")
    assert per_unit(Decimal("2"), 3) == Decimal("0.666666666667")


@pytest.mark.parametrize("factor", [0, None, -1, -1000])
def test_per_unit_has_no_value_without_a_positive_factor(factor):
    assert per_unit(Decimal("10"), factor) is None


# Decimal inputs


@pytest.mark.parametrize(
    "value,expected",
    [("10.25", "10.25"), (7, "7"), (Decimal("-3.5"), "-3.5"), ("1e24", "1E+24")],
)
def test_number_accepts_bounded_decimal_strings_integers_and_decimals(value, expected):
    assert str(number(value)) == expected


def test_number_bounds_are_inclusive():
    assert number("9" * 24) == Decimal("9" * 24)
    assert number("-1e24") == Decimal("-1e24")
    assert number("0." + "0" * 17 + "1") == Decimal("1e-18")
    assert number("1" * 24 + "." + "1" * 18) == Decimal("1" * 24 + "." + "1" * 18)
    assert number("0" * 79 + "1") == Decimal(1)


@pytest.mark.parametrize(
    "value,message",
    [
        (True, "provide a bounded decimal string"),
        (1.5, "provide a bounded decimal string"),
        (None, "provide a bounded decimal string"),
        ("1" * 81, "provide a bounded decimal string"),
        ("abc", "invalid decimal"),
        ("NaN", "decimal exceeds supported bounds"),
        ("Infinity", "decimal exceeds supported bounds"),
        ("1.0000001e24", "decimal exceeds supported bounds"),
        ("1e-19", "decimal exceeds supported bounds"),
    ],
)
def test_number_rejects_floats_booleans_and_out_of_bounds_values(value, message):
    with pytest.raises(ValueError, match=f"^{message}$"):
        number(value)


# COTAHIST records


def with_bytes(raw, start, value):
    data = bytearray(raw)
    data[start : start + len(value)] = value
    return bytes(data)


def test_record_fields_keep_exact_scaled_values_and_provenance():
    raw = with_bytes(record(close=3880, exercise_price=1234567), 217, b"1000000123456")
    row = cotahist.parse_record(raw, "source-sha", 7)
    assert row["close_scaled"] == 3880 and row["close"] == Decimal("38.80")
    assert row["exercise_price_scaled"] == 1234567
    assert row["exercise_price"] == Decimal("12345.67")
    assert row["exercise_points_scaled"] == 1000000123456
    assert row["exercise_points"] == Decimal("1000000.123456")
    assert row["quantity"] == 123 and row["quotation_factor"] == 1
    assert row["issuer_name"] == "PETROBRÁS"
    assert row["date"] == date(2024, 3, 5)
    assert row["source_hash"] == "source-sha" and row["source_line"] == 7
    assert row["row_hash"] == __import__("hashlib").sha256(raw).hexdigest()
    assert set(row) == {field.name for field in cotahist.SCHEMA}


@pytest.mark.parametrize(
    "raw_expiry,expected",
    [(b"20240315", date(2024, 3, 15)), (b"00000000", None), (b"99991231", None), (b" " * 8, None)],
)
def test_record_expiry_sentinels_mean_no_expiry(raw_expiry, expected):
    row = cotahist.parse_record(with_bytes(record(), 202, raw_expiry), "h", 1)
    assert row["expiry"] == expected


def test_record_length_is_exact():
    with pytest.raises(ValueError, match="^line 7: expected 245 bytes, got 244$"):
        cotahist.parse_record(record()[:-1], "h", 7)


@pytest.mark.parametrize(
    "start,field",
    [(108, "close"), (170, "turnover"), (152, "quantity"), (210, "quotation_factor")],
)
def test_record_numeric_fields_must_be_digits(start, field):
    with pytest.raises(ValueError, match=f"^line 3: invalid {field}$"):
        cotahist.parse_record(with_bytes(record(), start, b" "), "h", 3)


def test_record_exercise_points_must_be_digits():
    with pytest.raises(ValueError, match="^line 3: invalid exercise_points$"):
        cotahist.parse_record(with_bytes(record(), 229, b"-"), "h", 3)


@pytest.mark.parametrize("start,length", [(12, 12), (52, 4)])
def test_record_symbol_and_currency_are_both_required(start, length):
    with pytest.raises(ValueError, match="^line 4: missing identity$"):
        cotahist.parse_record(with_bytes(record(), start, b" " * length), "h", 4)


# COTAHIST files


def staged_parts(folder):
    return sorted(path.name for path in folder.glob("*.parquet"))


def test_stage_writes_numbered_bounded_parts_and_reports_progress(tmp_path):
    path = write_input(tmp_path, [record(day=f"202403{d:02d}") for d in (4, 5, 6, 7, 8)])
    calls = []
    result = cotahist.stage(path, tmp_path / "s", "sha", batch_rows=2, heartbeat=calls.append)
    assert staged_parts(tmp_path / "s") == ["000000.parquet", "000001.parquet", "000002.parquet"]
    assert len(calls) == 2 and all(isinstance(c, dict) for c in calls)
    assert calls[0] is result["counts"]
    assert result == {
        "counts": {"records": 5, "accepted": 5, "excluded_futures": 0, "quarantined": 0},
        "generated": date(2024, 3, 6),
        "frequency": 1,
        "dates": ["2024-03-04", "2024-03-05", "2024-03-06", "2024-03-07", "2024-03-08"],
        "period": "2024",
    }
    table = pq.ParquetDataset(tmp_path / "s").read()
    assert table.schema.equals(cotahist.SCHEMA)
    rows = table.to_pylist()
    assert {r["source_hash"] for r in rows} == {"sha"}
    assert sorted(r["source_line"] for r in rows) == [2, 3, 4, 5, 6]


def test_stage_flushes_a_full_batch_without_an_empty_trailing_part(tmp_path):
    path = write_input(tmp_path, [record(day="20240304"), record(day="20240305")])
    calls = []
    cotahist.stage(path, tmp_path / "s", "h", batch_rows=2, heartbeat=calls.append)
    assert staged_parts(tmp_path / "s") == ["000000.parquet"]
    assert len(calls) == 1


def test_stage_of_only_futures_writes_one_empty_typed_part(tmp_path):
    path = write_input(tmp_path, [record(market="050", bdi="70")])
    result = cotahist.stage(path, tmp_path / "s", "h")
    assert result["counts"] == {
        "records": 1,
        "accepted": 0,
        "excluded_futures": 1,
        "quarantined": 0,
    }
    assert staged_parts(tmp_path / "s") == ["000000.parquet"]
    table = pq.read_table(tmp_path / "s" / "000000.parquet")
    assert table.num_rows == 0 and table.schema.equals(cotahist.SCHEMA)


def test_stage_creates_missing_parents_and_reuses_an_existing_directory(tmp_path):
    path = write_input(tmp_path, [record()])
    cotahist.stage(path, tmp_path / "a" / "b", "h")
    existing = tmp_path / "existing"
    existing.mkdir()
    cotahist.stage(path, existing, "h")
    assert staged_parts(existing) == ["000000.parquet"]


def test_stage_default_part_size_is_the_configured_50000_rows(tmp_path):
    assert ResearchSettings().batch_rows == 50_000
    path = write_input(tmp_path, [record()] * 50_000)
    calls = []
    cotahist.stage(path, tmp_path / "s", "h", heartbeat=calls.append)
    assert len(calls) == 1 and staged_parts(tmp_path / "s") == ["000000.parquet"]


def test_stage_accepts_a_file_exactly_at_the_size_limit(tmp_path):
    path = write_input(tmp_path, [record()])
    size = path.stat().st_size
    cotahist.stage(path, tmp_path / "s", "h", max_uncompressed_bytes=size)
    with pytest.raises(ValueError, match="^oversized TXT$"):
        cotahist.stage(path, tmp_path / "t", "h", max_uncompressed_bytes=size - 1)


def test_stage_counts_the_bytes_it_reads_not_only_the_declared_size(tmp_path, monkeypatch):
    lines = file_bytes([record(), record(day="20240306")]).splitlines(keepends=True)

    class Growing:
        def __init__(self, *_):
            pass

        def __enter__(self):
            return iter(lines)

        def __exit__(self, *_):
            return False

    monkeypatch.setattr(cotahist, "records", Growing)
    limit = sum(map(len, lines))
    cotahist.stage(tmp_path / "x", tmp_path / "s", "h", max_uncompressed_bytes=limit)
    with pytest.raises(ValueError, match="^decompressed input exceeds limit$"):
        cotahist.stage(tmp_path / "x", tmp_path / "t", "h", max_uncompressed_bytes=limit - 1)
    with pytest.raises(ValueError, match="^decompressed input exceeds limit$"):
        cotahist.stage(tmp_path / "x", tmp_path / "u", "h", max_uncompressed_bytes=500)


def test_stage_strips_only_the_line_terminator(tmp_path):
    path = write_input(tmp_path, [record(distribution="10X")])
    cotahist.stage(path, tmp_path / "s", "h")
    assert pq.read_table(tmp_path / "s").to_pylist()[0]["distribution"] == "10X"


def lines_of(*records_):
    return b"\r\n".join(records_) + b"\r\n"


def header(year="2024", generated="20240306"):
    return ("00" + f"COTAHIST.{year}" + "BOVESPA " + generated).ljust(245).encode()


def trailer(count, year="2024", generated="20240306", filler=" "):
    text = "99" + f"COTAHIST.{year}" + "BOVESPA " + generated + str(count).zfill(11)
    return (text + filler).ljust(245).encode()


@pytest.mark.parametrize(
    "content,message",
    [
        (lines_of(record(), trailer(2)), "line 1: invalid record order/type"),
        (lines_of(header(), record(), header(), trailer(4)), "line 3: invalid record order/type"),
        (lines_of(header(), trailer(3), record()), "line 3: invalid record order/type"),
        (lines_of(header(), trailer(3), trailer(3)), "line 3: invalid record order/type"),
        (
            lines_of(header(), b"02" + record()[2:], trailer(3)),
            "line 2: invalid record order/type",
        ),
        (lines_of(header(), record()), "missing header/trailer"),
        (lines_of(header(), record(), trailer(4)), "header/trailer identity or count mismatch"),
        (
            lines_of(header(), record(), trailer(3, generated="20240307")),
            "header/trailer identity or count mismatch",
        ),
        (
            lines_of(header(year="20XX"), record(), trailer(3, year="20XX")),
            "unrecognized COTAHIST header period",
        ),
        (lines_of(header(), record()[:-1], trailer(3)), "line 2: invalid record length"),
    ],
)
def test_stage_requires_header_records_trailer_in_order(tmp_path, content, message):
    path = tmp_path / "COTAHIST_A2024.TXT"
    path.write_bytes(content)
    with pytest.raises(ValueError, match=f"^{message}$"):
        cotahist.stage(path, tmp_path / "s", "h")


def test_trailer_count_is_eleven_digits_and_filler_is_ignored(tmp_path):
    path = tmp_path / "COTAHIST_A2024.TXT"
    path.write_bytes(lines_of(header(), record(), trailer(3, filler="9")))
    assert cotahist.stage(path, tmp_path / "s", "h")["counts"]["accepted"] == 1


def test_trailer_count_uses_all_eleven_digits(tmp_path, monkeypatch):
    # A trailer for 10^10 lines without writing them: the line counter is replaced so the
    # trailer is numbered 10,000,000,000, the first count whose leading digit is not 0.
    path = tmp_path / "COTAHIST_A2024.TXT"
    path.write_bytes(lines_of(header(), record(), trailer(10**10)))

    def numbered(stream, start):
        lines = list(stream)
        numbers = [*range(start, start + len(lines) - 1), 10**10]
        return zip(numbers, lines, strict=True)

    monkeypatch.setattr(cotahist, "enumerate", numbered, raising=False)
    assert cotahist.stage(path, tmp_path / "s", "h")["counts"]["accepted"] == 1
    path.write_bytes(lines_of(header(), record(), trailer(10**10 + 1)))
    with pytest.raises(ValueError, match="^header/trailer identity or count mismatch$"):
        cotahist.stage(path, tmp_path / "t", "h")


@pytest.mark.parametrize(
    "name,generated,days,period,frequency",
    [
        ("COTAHIST_A2024.ZIP", "20240306", ["20240102", "20241231"], "2024", 1),
        ("cotahist_m032024.txt", "20240306", ["20240301", "20240329"], "032024", 2),
        ("COTAHIST_D05032024.TXT", "20240306", ["20240305"], "05032024", 3),
    ],
)
def test_filename_declares_frequency_and_coverage(
    tmp_path, name, generated, days, period, frequency
):
    path = write_input(tmp_path, [record(day=d) for d in days], name=name, generated=generated)
    result = cotahist.stage(path, tmp_path / "s", "h", input_name=name)
    assert (result["period"], result["frequency"]) == (period, frequency)
    assert result["generated"] == date(2024, 3, 6)


@pytest.mark.parametrize(
    "name,day",
    [
        ("COTAHIST_M032024.TXT", "20240401"),
        ("COTAHIST_M032024.TXT", "20230315"),
        ("COTAHIST_M032024.TXT", "20240229"),
        ("COTAHIST_D05032024.TXT", "20240306"),
        ("COTAHIST_D05032024.TXT", "20240304"),
        ("COTAHIST_A2024.TXT", "20250102"),
    ],
)
def test_trading_dates_outside_the_declared_period_are_rejected(tmp_path, name, day):
    path = write_input(tmp_path, [record(day=day)], name=name)
    with pytest.raises(ValueError, match="^trading date outside declared coverage$"):
        cotahist.stage(path, tmp_path / "s", "h", input_name=name)


@pytest.mark.parametrize(
    "name,message",
    [
        ("COTAHIST_A24.TXT", "supply an official COTAHIST_A/M/D filename"),
        ("COTAHIST_M2024.TXT", "supply an official COTAHIST_A/M/D filename"),
        ("COTAHIST_M0320241.TXT", "supply an official COTAHIST_A/M/D filename"),
        ("COTAHIST_D0503202.TXT", "supply an official COTAHIST_A/M/D filename"),
        ("COTAHIST_D050320241.TXT", "supply an official COTAHIST_A/M/D filename"),
        ("COTAHIST_A2024.CSV", "supply an official COTAHIST_A/M/D filename"),
        ("quotes.txt", "supply an official COTAHIST_A/M/D filename"),
        ("COTAHIST_A2023.TXT", "filename/header year mismatch"),
        ("COTAHIST_M032023.TXT", "filename/header year mismatch"),
        ("COTAHIST_D05032023.TXT", "filename/header year mismatch"),
    ],
)
def test_filename_must_be_official_and_match_the_header_year(tmp_path, name, message):
    path = write_input(tmp_path, [record()])
    with pytest.raises(ValueError, match=f"^{message}$"):
        cotahist.stage(path, tmp_path / "s", "h", input_name=name)


def test_without_a_filename_the_header_year_is_the_period(tmp_path):
    path = write_input(tmp_path, [record()])
    result = cotahist.stage(path, tmp_path / "s", "h")
    assert (result["period"], result["frequency"]) == ("2024", 1)


def test_conflicting_rows_for_one_identity_are_reported_with_both_lines(tmp_path):
    rows = [record(close=3880), record(close=3900)]
    path = write_input(tmp_path, rows)
    with pytest.raises(ValueError) as caught:
        cotahist.stage(path, tmp_path / "s", "h")
    identity = cotahist.parse_record(rows[0], "h", 2)["record_id"]
    assert str(caught.value) == (
        f"conflicting quote identity {identity} (2024-03-05, PETR4, market 010; "
        "source lines 2 and 3)"
    )


def test_identical_duplicate_rows_are_not_a_conflict(tmp_path):
    path = write_input(tmp_path, [record(), record()])
    assert cotahist.stage(path, tmp_path / "s", "h")["counts"]["accepted"] == 2


def test_stage_keeps_its_scratch_space_inside_the_output(tmp_path, monkeypatch):
    import tempfile

    seen = []
    original = tempfile.TemporaryDirectory

    def recording(*args, **kwargs):
        seen.append(kwargs.get("dir"))
        return original(*args, **kwargs)

    monkeypatch.setattr(tempfile, "TemporaryDirectory", recording)
    cotahist.stage(write_input(tmp_path, [record()]), tmp_path / "s", "h")
    assert seen == [tmp_path / "s"]


def test_zip_member_is_read_like_the_text_file(tmp_path):
    path = tmp_path / "COTAHIST_A2024.ZIP"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("COTAHIST_A2024.TXT", file_bytes([record()]))
    assert (
        cotahist.stage(path, tmp_path / "s", "h", input_name=path.name)["counts"]["accepted"] == 1
    )


# Pinned quote releases read from a local Delta table

POINTER_SQL = (
    "SELECT r.* FROM pointers p JOIN releases r ON r.id=p.release_id WHERE p.kind='quotes'"
)
RELEASE_SQL = "SELECT * FROM releases WHERE id=%s AND kind='quotes'"


def quote_rows(closes):
    rows = [
        ("q1", date(2024, 3, 4), "PETR4", "010", "BRL", 1),
        ("q2", date(2024, 3, 5), "PETR4", "010", "BRL", 1),
        ("q0", date(2024, 3, 5), "PETR4", "020", "BRL", 1),
        ("q3", date(2024, 3, 6), "PETR4", "010", "BRL", 1000),
        ("q4", date(2024, 3, 5), "VALE3", "010", "BRL", 1),
        ("q5", date(2024, 3, 5), "PETR4", "010", "USD", 1),
    ]
    return [
        {
            "record_id": r[0],
            "date": r[1],
            "symbol": r[2],
            "tpmerc": r[3],
            "currency": r[4],
            "quotation_factor": r[5],
            "close": Decimal(close),
        }
        for r, close in zip(rows, closes, strict=True)
    ]


@pytest.fixture(scope="module")
def delta_quotes(tmp_path_factory):
    import pyarrow as pa
    from deltalake import write_deltalake

    schema = pa.schema(
        [
            ("record_id", pa.string()),
            ("date", pa.date32()),
            ("symbol", pa.string()),
            ("tpmerc", pa.string()),
            ("currency", pa.string()),
            ("quotation_factor", pa.int64()),
            ("close", pa.decimal128(20, 2)),
        ]
    )
    uri = str(tmp_path_factory.mktemp("quotes") / "delta")
    first = ["38.80", "39.10", "1.00", "1000.00", "60.00", "7.50"]
    write_deltalake(uri, pa.Table.from_pylist(quote_rows(first), schema=schema))
    second = ["99.99"] * 6
    write_deltalake(uri, pa.Table.from_pylist(quote_rows(second), schema=schema), mode="overwrite")
    return uri


class Releases:
    """Release catalogue: the current pointer and releases by id."""

    def __init__(self, uri, pointer=("r-old", 0), known=(("r-old", 0), ("r-new", 1))):
        self.uri, self.pointer, self.known = uri, pointer, dict(known)
        self.calls = []

    def release(self, release_id, version):
        return {"id": release_id, "delta_uri": self.uri, "delta_version": version}

    def rows(self, sql, params=()):
        self.calls.append((sql, params))
        if sql == POINTER_SQL and not params:
            return [self.release(*self.pointer)] if self.pointer else []
        if sql == RELEASE_SQL:
            version = self.known.get(params[0])
            return [] if version is None else [self.release(params[0], version)]
        raise AssertionError(sql)


def test_quotes_come_from_the_pointed_release_at_its_pinned_delta_version(delta_quotes):
    db = Releases(delta_quotes)
    result = query.quote_query(db, symbol="PETR4", market="010", currency="BRL")
    assert db.calls == [(POINTER_SQL, ())]
    assert result["release"] == "r-old"
    assert [(r["record_id"], r["close"]) for r in result["rows"]] == [
        ("q1", Decimal("38.80")),
        ("q2", Decimal("39.10")),
        ("q3", Decimal("1000.00")),
    ]
    assert [r["close_per_unit"] for r in result["rows"]] == [
        Decimal("38.800000000000"),
        Decimal("39.100000000000"),
        Decimal("1.000000000000"),
    ]
    assert result["qualification"] == "Original currency and quotation basis; unadjusted prices"
    assert (result["limit"], result["offset"]) == (100, 0)
    assert set(result) == {"release", "rows", "qualification", "limit", "offset"}


def test_an_explicit_release_is_read_at_its_own_version(delta_quotes):
    db = Releases(delta_quotes)
    result = query.quote_query(db, release="r-new", symbol="VALE3")
    assert db.calls == [(RELEASE_SQL, ("r-new",))]
    assert result["release"] == "r-new"
    assert [(r["record_id"], r["close"]) for r in result["rows"]] == [("q4", Decimal("99.99"))]


@pytest.mark.parametrize("release,pointer", [("r-missing", ("r-old", 0)), (None, None)])
def test_no_release_means_no_rows(delta_quotes, release, pointer):
    result = query.quote_query(Releases(delta_quotes, pointer=pointer), release=release)
    assert result == {"release": None, "rows": [], "qualification": "No quote release published"}


@pytest.mark.parametrize(
    "filters,expected",
    [
        ({}, ["q1", "q0", "q2", "q4", "q5", "q3"]),
        ({"symbol": "VALE3"}, ["q4"]),
        ({"market": "020"}, ["q0"]),
        ({"currency": "USD"}, ["q5"]),
        ({"date_from": date(2024, 3, 5)}, ["q0", "q2", "q4", "q5", "q3"]),
        ({"date_to": date(2024, 3, 5)}, ["q1", "q0", "q2", "q4", "q5"]),
        ({"date_from": date(2024, 3, 5), "date_to": date(2024, 3, 5)}, ["q0", "q2", "q4", "q5"]),
        ({"limit": 2}, ["q1", "q0"]),
        ({"limit": 2, "offset": 2}, ["q2", "q4"]),
        ({"symbol": "PETR4", "currency": "BRL", "date_from": date(2024, 3, 6)}, ["q3"]),
    ],
)
def test_filters_select_by_symbol_market_currency_and_inclusive_dates(
    delta_quotes, filters, expected
):
    result = query.quote_query(Releases(delta_quotes), **filters)
    assert [r["record_id"] for r in result["rows"]] == expected
    assert result["limit"] == filters.get("limit", 100)
    assert result["offset"] == filters.get("offset", 0)


def test_a_reversed_date_interval_is_refused(delta_quotes):
    with pytest.raises(ValueError, match="^date interval reversed$"):
        query.quote_query(
            Releases(delta_quotes), date_from=date(2024, 3, 6), date_to=date(2024, 3, 5)
        )


def test_arrow_export_appends_the_exact_per_unit_price(delta_quotes):
    import pyarrow as pa

    release, table = query.quote_query(Releases(delta_quotes), symbol="PETR4", arrow=True)
    assert release == "r-old"
    assert table.column_names[-1] == "close_per_unit"
    assert table.schema.field("close_per_unit").type == pa.decimal128(38, 12)
    assert table["record_id"].to_pylist() == ["q1", "q0", "q2", "q5", "q3"]
    assert table["close_per_unit"].to_pylist() == [
        Decimal("38.800000000000"),
        Decimal("1.000000000000"),
        Decimal("39.100000000000"),
        Decimal("7.500000000000"),
        Decimal("1.000000000000"),
    ]


# Exact quote comparison


def quote(day, close, **identity):
    return {
        "date": date(2026, 1, day),
        "isin": "BRTESTACNOR1",
        "symbol": "TEST3",
        "tpmerc": "010",
        "currency": "BRL",
        "distribution": "100",
        "quotation_factor": 1,
        "close_per_unit": Decimal(close) if close is not None else None,
        **identity,
    }


def compare(monkeypatch, rows, release="r1"):
    calls = []

    def fake(db, **kwargs):
        calls.append((db, kwargs))
        return {"release": release, "rows": rows}

    monkeypatch.setattr(query, "quote_query", fake)
    result = compare_quotes(
        "db", symbol="TEST3", market="010", date_from="d1", date_to="d2", release="r1"
    )
    return result, calls


def test_comparison_reports_exact_change_from_first_to_last_observation(monkeypatch):
    rows = [quote(2, "3"), quote(3, "100"), quote(5, "4")]
    result, calls = compare(monkeypatch, rows, release="release-7")
    assert calls == [
        (
            "db",
            {
                "symbol": "TEST3",
                "market": "010",
                "date_from": "d1",
                "date_to": "d2",
                "release": "r1",
                "limit": 10001,
            },
        )
    ]
    assert result == {
        "release": "release-7",
        "first": rows[0],
        "last": rows[2],
        "difference_per_unit": "1",
        "price_change_percent": exact(1, 3, 100),
        "qualification": "Unadjusted observed price change, not total return. Equal identity "
        "does not establish absence of corporate actions. No reinvestment, costs or "
        "historical strategy claim.",
    }


def test_comparison_accepts_exactly_10000_rows(monkeypatch):
    rows = [quote(1, "1")] + [
        {**quote(1, "1"), "date": date(2000, 1, 1).fromordinal(700000 + i)} for i in range(9999)
    ]
    rows[-1]["close_per_unit"] = Decimal("2")
    assert compare(monkeypatch, rows)[0]["difference_per_unit"] == "1"


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [quote(1, "1")]
        + [{**quote(1, "1"), "date": date.fromordinal(700000 + i)} for i in range(10000)],
    ],
)
def test_comparison_needs_one_to_10000_rows(monkeypatch, rows):
    with pytest.raises(ValueError, match="^no quotes or more than 10,000 rows; narrow interval$"):
        compare(monkeypatch, rows)


@pytest.mark.parametrize(
    "rows",
    [
        [quote(1, "1"), quote(2, "2", quotation_factor=1000)],
        [quote(1, "1"), quote(2, "2", currency="USD")],
        [quote(1, "1"), quote(2, "2", distribution="101")],
        [quote(1, "1", isin=""), quote(2, "2", isin="")],
        [quote(1, "1"), quote(1, "2"), quote(2, "3")],
    ],
)
def test_comparison_refuses_identity_or_basis_changes(monkeypatch, rows):
    message = "^ambiguous instrument identity, duplicate dates or quotation basis changed$"
    with pytest.raises(ValueError, match=message):
        compare(monkeypatch, rows)


@pytest.mark.parametrize(
    "rows",
    [
        [quote(1, "1")],
        [quote(1, "0"), quote(2, "2")],
        [quote(1, None), quote(2, "2")],
        [quote(1, "1"), quote(2, None)],
    ],
)
def test_comparison_needs_two_dates_and_a_nonzero_starting_basis(monkeypatch, rows):
    message = "^two distinct dates and nonzero valid quotation basis required$"
    with pytest.raises(ValueError, match=message):
        compare(monkeypatch, rows)


def test_comparison_allows_a_zero_final_price(monkeypatch):
    result, _ = compare(monkeypatch, [quote(1, "1"), quote(2, "0")])
    assert result["price_change_percent"] == "-100"
    result, _ = compare(monkeypatch, [quote(1, "1"), quote(2, "1")])
    assert result["price_change_percent"] == "0"


# Portfolio weights and scenarios


def position(instrument, value, asset_class="equity", **extra):
    return {
        "instrument_id": instrument,
        "market": "010",
        "currency": "BRL",
        "asset_class": asset_class,
        "market_value": value,
        **extra,
    }


def book(*positions, scenarios=None):
    body = {"currency": "BRL", "valuation_date": "2026-10-18", "positions": list(positions)}
    if scenarios is not None:
        body["scenarios"] = scenarios
    return body


def test_portfolio_weights_exposures_and_scenarios_are_exact():
    from ofl.research.db import digest

    body = book(
        position("A", "1"),
        position("B", "1", market="account"),
        position("C", "1", asset_class="cash"),
        scenarios=[{"name": "Stress", "shocks": {"equity": "-10", "cash": "5"}}],
    )
    third = exact(1, 3, 100)
    assert portfolio(body) == {
        "valuation_date": "2026-10-18",
        "currency": "BRL",
        "total_value": "3",
        "positions": [
            {
                "instrument_id": "A",
                "market": "010",
                "currency": "BRL",
                "asset_class": "equity",
                "market_value": "1",
                "weight_percent": third,
            },
            {
                "instrument_id": "B",
                "market": "account",
                "currency": "BRL",
                "asset_class": "equity",
                "market_value": "1",
                "weight_percent": third,
            },
            {
                "instrument_id": "C",
                "market": "010",
                "currency": "BRL",
                "asset_class": "cash",
                "market_value": "1",
                "weight_percent": third,
            },
        ],
        "exposures": [
            {"asset_class": "equity", "market_value": "2", "weight_percent": exact(2, 3, 100)},
            {"asset_class": "cash", "market_value": "1", "weight_percent": third},
        ],
        "largest_position_percent": third,
        "scenarios": [
            {
                "name": "Stress",
                "assumptions": {"equity": "-10", "cash": "5"},
                "value_change": "-0.15",
                "projected_value": "2.85",
                "change_percent": "-5.00",
            }
        ],
        "input_sha256": digest(body),
        "method": "ofl/valued-long-only-portfolio/1",
        "qualification": "User-supplied valuations and classifications, not verified market "
        "quotes. Linear hypothetical shocks are assumptions, not forecasts, suitability "
        "recommendations or historical performance. No automatic FX conversion, look-through, "
        "derivatives or corporate-action adjustment.",
    }


def test_largest_position_is_the_heaviest_weight():
    result = portfolio(book(position("A", "1"), position("B", "2"), position("C", "1")))
    assert result["largest_position_percent"] == "50.0"
    assert [p["weight_percent"] for p in result["positions"]] == ["25.00", "50.0", "25.00"]


def test_portfolio_defaults():
    body = book(
        {"instrument_id": "A", "market": "010", "currency": "BRL", "market_value": "0"},
        position("B", "2"),
    )
    result = portfolio(body)
    assert result["scenarios"] == []
    assert [p["asset_class"] for p in result["positions"]] == ["unknown", "equity"]
    assert result["positions"][0]["weight_percent"] == "0"


def test_portfolio_accepts_the_documented_bounds():
    many = [position(f"P{i}", "1") for i in range(200)]
    scenarios = [{"name": "n" * 120, "shocks": {"equity": "1000"}}] * 19 + [
        {"name": "x", "shocks": {"equity": "-100"}}
    ]
    result = portfolio({**book(*many, scenarios=scenarios), "currency": "BRL"})
    assert len(result["positions"]) == 200 and len(result["scenarios"]) == 20
    assert result["scenarios"][-1]["projected_value"] == "0"
    one = portfolio(
        book(position("A", "1", asset_class="a" * 80), position("B", "1", asset_class="x"))
    )
    assert one["total_value"] == "2"
    long_ids = position("i" * 128, "1", market="m" * 128)
    assert portfolio(book(long_ids))["positions"][0]["instrument_id"] == "i" * 128
    currency = "C" * 16
    single = {**position("A", "1"), "currency": currency}
    assert portfolio({**book(single), "currency": currency})["currency"] == currency
    assert portfolio({**book({**single, "currency": "C"}), "currency": "C"})["currency"] == "C"
    assert portfolio(book(position("A", "1"), scenarios=[{"name": "s", "shocks": {"equity": "0"}}]))


@pytest.mark.parametrize(
    "change,message",
    [
        (lambda b: b.pop("positions"), "provide 1–200 valued positions"),
        (lambda b: b.update(positions=[]), "provide 1–200 valued positions"),
        (lambda b: b.update(positions={"A": 1}), "provide 1–200 valued positions"),
        (
            lambda b: b.update(positions=[position(f"P{i}", "1") for i in range(201)]),
            "provide 1–200 valued positions",
        ),
        (lambda b: b.update(currency=""), "base currency required"),
        (lambda b: b.update(currency="C" * 17), "base currency required"),
        (lambda b: b.update(currency=None), "base currency required"),
        (lambda b: b.pop("valuation_date"), "valuation date required"),
        (lambda b: b.update(valuation_date="18/10/2026"), "valuation date required"),
        (lambda b: b["positions"].append("A"), "position must be an object"),
        (
            lambda b: b["positions"][0].pop("instrument_id"),
            "distinct instrument identity, market and currency required",
        ),
        (
            lambda b: b["positions"][0].update(market=" "),
            "distinct instrument identity, market and currency required",
        ),
        (
            lambda b: b["positions"][0].update(instrument_id="i" * 129),
            "distinct instrument identity, market and currency required",
        ),
        (
            lambda b: b["positions"].append(position("A", "2")),
            "distinct instrument identity, market and currency required",
        ),
        (
            lambda b: b["positions"][0].update(currency="USD"),
            "convert positions explicitly to one valuation currency before analysis",
        ),
        (
            lambda b: b["positions"][0].update(market_value="-0.01"),
            "this descriptive workflow supports long-only valued holdings",
        ),
        (lambda b: b["positions"][0].update(asset_class=""), "bounded asset_class required"),
        (lambda b: b["positions"][0].update(asset_class="a" * 81), "bounded asset_class required"),
        (lambda b: b["positions"][0].update(asset_class=3), "bounded asset_class required"),
        (lambda b: b["positions"][0].update(market_value="0"), "positive portfolio value required"),
        (lambda b: b.update(scenarios={}), "at most 20 scenarios"),
        (
            lambda b: b.update(scenarios=[{"name": "s", "shocks": {"equity": "0"}}] * 21),
            "at most 20 scenarios",
        ),
        (lambda b: b.update(scenarios=["s"]), "scenario name required"),
        (lambda b: b.update(scenarios=[{"shocks": {"equity": "0"}}]), "scenario name required"),
        (
            lambda b: b.update(scenarios=[{"name": "", "shocks": {"equity": "0"}}]),
            "scenario name required",
        ),
        (
            lambda b: b.update(scenarios=[{"name": "n" * 121, "shocks": {"equity": "0"}}]),
            "scenario name required",
        ),
        (
            lambda b: b.update(scenarios=[{"name": "s"}]),
            "specify an explicit percentage shock for every asset class, including zero",
        ),
        (
            lambda b: b.update(scenarios=[{"name": "s", "shocks": {"equity": "0", "cash": "0"}}]),
            "specify an explicit percentage shock for every asset class, including zero",
        ),
        (
            lambda b: b.update(scenarios=[{"name": "s", "shocks": {"equity": "-100.01"}}]),
            r"scenario shock outside supported range \[-100,1000\] percent",
        ),
        (
            lambda b: b.update(scenarios=[{"name": "s", "shocks": {"equity": "1000.01"}}]),
            r"scenario shock outside supported range \[-100,1000\] percent",
        ),
    ],
)
def test_portfolio_rejects_inputs_outside_the_contract(change, message):
    body = book(position("A", "1"))
    change(body)
    with pytest.raises(ValueError, match=f"^{message}$"):
        portfolio(body)


def test_portfolio_value_parsing_uses_the_bounded_decimal_rules():
    with pytest.raises(ValueError, match="^provide a bounded decimal string$"):
        portfolio(book(position("A", 1.5)))


def test_portfolio_precision_is_50_significant_digits():
    result = portfolio(book(position("A", "1"), position("B", "2", asset_class="cash")))
    assert result["positions"][0]["weight_percent"] == exact(1, 3, 100)
    assert len(result["positions"][0]["weight_percent"].replace(".", "")) == 50


# Reviewed table cells


MAPPING = {
    "header_rows": 1,
    "date": {"column": 0},
    "subject": {"column": 1},
    "metric": {"constant": " Close "},
    "value": {"column": 2},
    "unit": {"constant": "BRL"},
    "market": {"constant": "010"},
}


def test_reviewed_cells_become_exact_observations():
    cells = [["Date", "Subject", "Value"], ["2024-03-05", " PETR4 ", "1234.5600"], ["", None, " "]]
    assert normalize(cells, MAPPING) == [
        {
            "date": date(2024, 3, 5),
            "date_end": date(2024, 3, 5),
            "date_precision": "day",
            "date_text": "2024-03-05",
            "market": "010",
            "value": Decimal("1234.5600"),
            "subject": "PETR4",
            "metric": "Close",
            "unit": "BRL",
            "currency": None,
            "row": 1,
            "value_column": 2,
        }
    ]


def test_header_rows_and_row_numbers_follow_the_source_table():
    cells = [["t"], ["h"], ["2024-03-05", "A", "1"], [None, None, None], ["2024-03-06", "B", "2"]]
    rows = normalize(cells, {**MAPPING, "header_rows": 2})
    assert [(r["row"], r["subject"]) for r in rows] == [(2, "A"), (4, "B")]
    without_header = {k: v for k, v in MAPPING.items() if k != "header_rows"}
    assert [r["row"] for r in normalize(cells[1:], without_header)] == [1, 3]
    assert [r["row"] for r in normalize(cells[2:], {**MAPPING, "header_rows": 0})] == [0, 2]


def test_separators_parentheses_currency_and_constants():
    mapping = {
        **MAPPING,
        "decimal_separator": ",",
        "thousands_separator": ".",
        "currency": {"constant": " BRL "},
        "value": {"constant": "(1.234,50)"},
        "date_format": "%d/%m/%Y",
    }
    row = normalize([["h"], ["05/03/2024", "PETR4", "x"]], mapping)[0]
    assert str(row["value"]) == "-1234.50"
    assert row["currency"] == "BRL"
    assert row["value_column"] is None
    spaced = {**MAPPING, "thousands_separator": " "}
    assert normalize([["h"], ["2024-03-05", "A", "1 000.5"]], spaced)[0]["value"] == Decimal(
        "1000.5"
    )


def test_currency_may_come_from_a_column():
    mapping = {**MAPPING, "currency": {"column": 3}}
    row = normalize([["h"], ["2024-03-05", "A", "1", " USD "]], mapping)[0]
    assert row["currency"] == "USD"


def test_futures_rows_are_skipped_and_other_markets_kept():
    cells = [
        ["h"],
        ["2024-03-05", "A", "1", "050"],
        ["2024-03-05", "B", "2", "060"],
        ["bad", "C", "x", "060"],
        ["2024-03-05", "D", "3", "070"],
    ]
    rows = normalize(cells, {**MAPPING, "market": {"column": 3}})
    assert [(r["subject"], r["market"]) for r in rows] == [("D", "070")]


@pytest.mark.parametrize(
    "cells,mapping,message",
    [
        ([["h"]], {**MAPPING, "header_rows": -1}, "header_rows must be a nonnegative integer"),
        ([["h"]], {**MAPPING, "header_rows": "1"}, "header_rows must be a nonnegative integer"),
        (
            [["h"]],
            {k: v for k, v in MAPPING.items() if k != "unit"},
            "mapping requires date, subject, metric, value, unit, market",
        ),
        (
            [["h"], ["2024-03-05", "A", "1"]],
            {**MAPPING, "unit": "BRL"},
            "unit: specify column or constant",
        ),
        (
            [["h"], ["2024-03-05", "A", "1"]],
            {**MAPPING, "unit": {}},
            "unit: specify column or constant",
        ),
        ([["h"], ["2024-03-05", "A"]], MAPPING, "value: invalid column"),
        (
            [["h"], ["2024-03-05", "A", "1"]],
            {**MAPPING, "value": {"column": -1}},
            "value: invalid column",
        ),
        (
            [["h"], ["2024-03-05", "A", "1"]],
            {**MAPPING, "value": {"column": "2"}},
            "value: invalid column",
        ),
        (
            [["h"], ["2024-03-05", "A", "1"]],
            {**MAPPING, "market": {"constant": "999"}},
            "row 1: unknown market",
        ),
        ([["h"], ["2024-03-05", "A", "abc"]], MAPPING, "row 1: invalid numeric value"),
        ([["h"], ["2024-03-05", "A", "(12"]], MAPPING, "row 1: invalid numeric value"),
        ([["h"], ["2024-03-05", "A", "12)"]], MAPPING, "row 1: invalid numeric value"),
        ([["h"], ["2024-03-05", "A", "1XXXX0"]], MAPPING, "row 1: invalid numeric value"),
        ([["h"], ["2024-03-05", "A", "1,5"]], MAPPING, "row 1: invalid numeric value"),
        (
            [["h"], ["2024-03-05", "A", "1"]],
            {**MAPPING, "decimal_separator": ";"},
            "invalid decimal/thousands separators",
        ),
        (
            [["h"], ["2024-03-05", "A", "1"]],
            {**MAPPING, "thousands_separator": "."},
            "invalid decimal/thousands separators",
        ),
        ([["h"], ["2024-03-05", "A", "NaN"]], MAPPING, "value outside exact observation precision"),
        (
            [["h"], ["2024-03-05", "A", "1e26"]],
            MAPPING,
            "value outside exact observation precision",
        ),
        (
            [["h"], ["2024-03-05", "A", "-1e26"]],
            MAPPING,
            "value outside exact observation precision",
        ),
        (
            [["h"], ["2024-03-05", "A", "1e-13"]],
            MAPPING,
            "value outside exact observation precision",
        ),
        ([["h"], ["2024-03-05", "", "1"]], MAPPING, "subject, metric and unit must be resolved"),
        (
            [["h"], ["2024-03-05", "Unknown", "1"]],
            MAPPING,
            "subject, metric and unit must be resolved",
        ),
        ([["h"], ["2024-03-05", "?", "1"]], MAPPING, "subject, metric and unit must be resolved"),
        (
            [["h"], ["2024-03-05", "A", "1"]],
            {**MAPPING, "metric": {"constant": "unknown"}},
            "subject, metric and unit must be resolved",
        ),
        (
            [["h"], ["2024-03-05", "A", "1"]],
            {**MAPPING, "unit": {"constant": " "}},
            "subject, metric and unit must be resolved",
        ),
    ],
)
def test_reviewed_cells_reject_unresolved_or_inexact_values(cells, mapping, message):
    with pytest.raises(ValueError, match=f"^{message}$"):
        normalize(cells, mapping)


def test_value_bounds_are_inclusive_of_the_stored_precision():
    rows = normalize(
        [["h"], ["2024-03-05", "A", "9" * 26], ["2024-03-05", "B", "0.000000000001"]], MAPPING
    )
    assert [r["value"] for r in rows] == [Decimal("9" * 26), Decimal("1e-12")]


def test_market_column_holds_the_cell_text_not_the_label():
    rows = normalize(
        [["h"], ["2024-03-05", "A", "1", " 010 "]], {**MAPPING, "market": {"column": 3}}
    )
    assert rows[0]["market"] == "010"


@pytest.mark.parametrize(
    "text,format,expected",
    [
        ("2024-02-10", "%Y-%m-%d", (date(2024, 2, 10), date(2024, 2, 10), "day")),
        ("10/02/2024", "%d/%m/%Y", (date(2024, 2, 10), date(2024, 2, 10), "day")),
        ("2024-02", "%Y-%m", (date(2024, 2, 1), date(2024, 2, 29), "month")),
        ("02/2023", "%m/%Y", (date(2023, 2, 1), date(2023, 2, 28), "month")),
        ("2024", "%Y", (date(2024, 1, 1), date(2024, 12, 31), "year")),
    ],
)
def test_observation_period_spans_the_stated_precision(text, format, expected):
    assert observation_period(text, format) == expected


@pytest.mark.parametrize("format", ["%y-%m-%d", "%Y-%M-%D", "%d/%m/%y", "%y-%m", "%Y/%m/%d", ""])
def test_observation_period_accepts_only_documented_formats(format):
    with pytest.raises(ValueError, match="^unsupported date format$"):
        observation_period("2024-02-10", format)


def test_date_format_is_taken_from_the_mapping():
    rows = normalize([["h"], ["2024-02", "A", "1"]], {**MAPPING, "date_format": "%Y-%m"})
    assert (rows[0]["date"], rows[0]["date_end"], rows[0]["date_precision"]) == (
        date(2024, 2, 1),
        date(2024, 2, 29),
        "month",
    )
