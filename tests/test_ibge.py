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


def test_fetch_reads_the_table_route(monkeypatch):
    from ofl.ingestion import ibge

    seen = {}

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return [{"resultados": [{"series": [{"serie": {"202607": "5.3", "202608": "5.3"}}]}]}]

    def _get(url, timeout):
        seen["url"] = url
        return _Resp()

    monkeypatch.setattr(ibge.requests, "get", _get)
    out = ibge.fetch_ibge(6381, 4099)
    assert "/agregados/6381/periodos/all/variaveis/4099" in seen["url"]
    assert out.height == 2
    assert out.row(1, named=True) == {"date": date(2026, 8, 1), "value": 5.3}
