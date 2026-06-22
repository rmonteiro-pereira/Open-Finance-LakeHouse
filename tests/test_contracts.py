from datetime import date

import polars as pl
import pytest

from ofl.quality.contracts import validate_observation

SCHEMA = {"series_id": pl.String, "date": pl.Date, "value": pl.Float64}


def _frame(rows):
    return pl.DataFrame(rows, schema=SCHEMA)


def test_good_frame_passes():
    df = _frame({"series_id": ["selic", "selic"], "date": [date(2024, 1, 1), date(2024, 1, 2)], "value": [11.0, 11.25]})
    assert validate_observation(df, max_value=100.0).height == 2


def test_out_of_range_rejected():
    df = _frame({"series_id": ["selic", "selic"], "date": [date(2024, 1, 1), date(2024, 1, 2)], "value": [11.0, 250.0]})
    with pytest.raises(ValueError):
        validate_observation(df, max_value=100.0)


def test_null_date_rejected():
    df = _frame({"series_id": ["x"], "date": [None], "value": [1.0]})
    with pytest.raises(Exception):
        validate_observation(df)


def test_duplicate_key_rejected():
    df = _frame({"series_id": ["s", "s"], "date": [date(2024, 1, 1), date(2024, 1, 1)], "value": [1.0, 2.0]})
    with pytest.raises(Exception):
        validate_observation(df)
