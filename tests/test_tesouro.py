from datetime import date

import polars as pl

from ofl.ingestion.tesouro import _to_long


def test_to_long_parses_brazilian_numbers_and_dates():
    raw = pl.DataFrame(
        {
            "Tipo Titulo": ["Tesouro IPCA+ 2029", "Tesouro IPCA+ 2029"],
            "Data Vencimento": ["15/08/2029", "15/08/2029"],
            "Data Base": ["02/01/2024", "03/01/2024"],
            "Taxa Compra Manha": ["5,50", "5,52"],
            "Taxa Venda Manha": ["5,60", "5,62"],
            "PU Compra Manha": ["1.234,56", "1.230,00"],
            "PU Venda Manha": ["1.233,00", "1.229,00"],
        }
    )
    out = _to_long(raw)
    assert out.columns == ["bond", "maturity", "date", "buy_rate", "sell_rate", "buy_price", "sell_price"]
    assert out["date"].dtype == pl.Date and out["maturity"].dtype == pl.Date
    first = out.row(0, named=True)
    assert first["date"] == date(2024, 1, 2)
    assert first["sell_rate"] == 5.60
    assert first["buy_price"] == 1234.56  # thousand separator handled
