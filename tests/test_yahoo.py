from datetime import date

import polars as pl

from ofl.ingestion.yahoo import _to_long


def test_to_long_normalizes_ohlcv():
    raw = pl.DataFrame(
        {
            "Date": [date(2024, 1, 2), date(2024, 1, 3)],
            "Open": [10.0, 10.5],
            "High": [11.0, 11.2],
            "Low": [9.8, 10.4],
            "Close": [10.8, 11.0],
            "Adj Close": [10.8, 11.0],
            "Volume": [1000, 1200],
        }
    )
    out = _to_long(raw, "BOVA11.SA")
    assert out.columns == ["symbol", "date", "open", "high", "low", "close", "volume"]
    assert out["date"].dtype == pl.Date
    assert out["symbol"].to_list() == ["BOVA11.SA"] * 2
    assert "adj_close" not in out.columns  # Adj Close dropped
