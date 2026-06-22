from datetime import date

import polars as pl

from ofl.ingestion.ibge import _normalize


def test_normalize_flattens_nested_payload():
    payload = [
        {
            "resultados": [
                {
                    "series": [
                        {"serie": {"202401": "7,5", "202402": "7,8", "202403": "...", "202404": "7,6"}}
                    ]
                }
            ]
        }
    ]
    out = _normalize(payload)
    assert out.columns == ["date", "value"]
    assert out["date"].dtype == pl.Date
    assert out.height == 3  # "..." skipped
    assert out.row(0, named=True) == {"date": date(2024, 1, 1), "value": 7.5}
