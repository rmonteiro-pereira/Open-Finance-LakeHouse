from datetime import date

import pytest

from ofl.ingestion import anbima
from ofl.ingestion.anbima import _parse, fetch_anbima_ima
from ofl.transform.keys import instrument_id


def test_parse_maps_tpf_records():
    records = [
        {
            "codigo_selic": "100000",
            "data_referencia": "2026-06-19",
            "data_vencimento": "2029-01-01",
            "taxa_compra": "13,10",
            "taxa_venda": "13,00",
            "taxa_indicativa": "13,05",
            "pu": "950,123456",
        }
    ]
    out = _parse(records)
    assert out.columns == [
        "bond",
        "maturity",
        "date",
        "buy_rate",
        "sell_rate",
        "buy_price",
        "sell_price",
        "instrument_id",
    ]
    r = out.row(0, named=True)
    # Stamped at landing, from the ANBIMA namespace — so an ANBIMA row can never share an
    # identifier with a Tesouro row even if the labels were to coincide.
    assert r["instrument_id"] == instrument_id("anbima", "100000", date(2029, 1, 1))
    assert r["date"] == date(2026, 6, 19)
    assert r["maturity"] == date(2029, 1, 1)
    assert r["sell_rate"] == 13.05  # taxa_indicativa -> the curve yield
    assert r["buy_rate"] == 13.10
    assert r["buy_price"] == pytest.approx(950.123456)


def test_parse_empty():
    assert _parse([]).height == 0


def test_fetch_ima_parses_levels(monkeypatch):
    payload = [
        {"indice": "IMA-B", "data_referencia": "2026-06-19", "numero_indice": 5234.123456},
        {"indice": "IRF-M", "data_referencia": "2026-06-19", "numero_indice": 17890.5},
    ]

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return payload

    monkeypatch.setattr(anbima.requests, "get", lambda *a, **k: _Resp())
    df = fetch_anbima_ima("cid", "tok")
    assert set(df["indice"].to_list()) == {"IMA-B", "IRF-M"}
    imab = df.filter(df["indice"] == "IMA-B").row(0, named=True)
    assert imab["date"] == date(2026, 6, 19)
    assert imab["value"] == pytest.approx(5234.123456)
