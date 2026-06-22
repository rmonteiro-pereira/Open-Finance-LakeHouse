from datetime import date

import polars as pl

from ofl.ingestion.ipea import _parse


def test_parse_handles_mixed_int_float_and_timestamps():
    rows = [
        {"VALDATA": "2024-01-01T00:00:00-03:00", "VALVALOR": 5055},      # int
        {"VALDATA": "2024-02-01T00:00:00-03:00", "VALVALOR": 5055.2},    # float
        {"VALDATA": "2024-02-01T00:00:00-03:00", "VALVALOR": 5060.0},    # dup date -> last wins
        {"VALDATA": "2024-03-01T00:00:00-03:00", "VALVALOR": None},      # null kept
    ]
    out = _parse(rows)
    assert out.columns == ["date", "value"]
    assert out["date"].dtype == pl.Date
    assert out["value"].dtype == pl.Float64
    assert out.row(0, named=True) == {"date": date(2024, 1, 1), "value": 5055.0}
    assert out.filter(pl.col("date") == date(2024, 2, 1)).row(0, named=True)["value"] == 5060.0


def test_parse_empty():
    assert _parse([]).height == 0
