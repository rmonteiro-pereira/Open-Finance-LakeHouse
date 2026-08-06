from datetime import date

import pytest

from ofl.ingestion.tesouro import _to_long
from ofl.transform.keys import instrument_id

import polars as pl


def _raw(**overrides) -> pl.DataFrame:
    """A frame shaped like the real CSV.

    The label carries no year. That matters: this fixture used to say
    ``"Tesouro IPCA+ 2029"``, and with the maturity baked into ``Tipo Titulo`` the shipped
    key ``(bond, date)`` looked sufficient. It is not — ``Data Vencimento`` is a separate
    column precisely because one label spans several maturities, and the old fixture is
    the reason no test ever caught the collision.
    """
    base = {
        "Tipo Titulo": ["Tesouro IPCA+", "Tesouro IPCA+"],
        "Data Vencimento": ["15/08/2029", "15/08/2029"],
        "Data Base": ["02/01/2024", "03/01/2024"],
        "Taxa Compra Manha": ["5,50", "5,52"],
        "Taxa Venda Manha": ["5,60", "5,62"],
        "PU Compra Manha": ["1.234,56", "1.230,00"],
        "PU Venda Manha": ["1.233,00", "1.229,00"],
    }
    base.update(overrides)
    return pl.DataFrame(base)


def test_to_long_parses_brazilian_numbers_and_dates():
    out = _to_long(_raw())
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
    assert out["date"].dtype == pl.Date and out["maturity"].dtype == pl.Date
    first = out.row(0, named=True)
    assert first["date"] == date(2024, 1, 2)
    assert first["sell_rate"] == 5.60
    assert first["buy_price"] == 1234.56  # thousand separator handled


def test_instrument_id_is_stamped_at_landing():
    """Computed once, in Polars, and carried downstream — Spark never recomputes it."""
    out = _to_long(_raw())
    assert out["instrument_id"][0] == instrument_id("tesouro", "Tesouro IPCA+", date(2029, 8, 15))


def test_two_maturities_of_one_label_stay_separable():
    """The defect, as data. Same ``Tipo Titulo``, same trading day, two maturities."""
    raw = _raw(
        **{
            "Data Vencimento": ["15/08/2029", "15/05/2035"],
            "Data Base": ["02/01/2024", "02/01/2024"],
        }
    )
    out = _to_long(raw)
    assert out["instrument_id"].n_unique() == 2
    # ... and the key that shipped cannot tell them apart:
    assert out.select("bond", "date").n_unique() == 1


def test_row_without_maturity_never_reaches_bronze():
    """A key component parsed with ``strict=False`` must not become a null identifier."""
    raw = _raw(**{"Data Vencimento": ["15/08/2029", "not-a-date"]})
    out = _to_long(raw)
    assert out.height == 1
    assert out["instrument_id"].null_count() == 0
