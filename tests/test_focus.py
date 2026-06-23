import pytest

from ofl.ingestion import bacen_focus
from ofl.registry import Series


def _series(extra):
    return Series(key="focus_x", domain="rates", handler="bacen_focus", name="x", extra=extra)


def _capture(monkeypatch, rows):
    monkeypatch.setattr(bacen_focus, "fetch_focus", lambda extra: rows)
    captured = {}
    monkeypatch.setattr(
        bacen_focus, "land_bronze", lambda s, df: captured.update(df=df) or {"rows": df.height}
    )
    return captured


def test_current_year_horizon_keeps_one_row_per_date(monkeypatch):
    rows = [
        {"Data": "2026-06-19", "DataReferencia": "2026", "Mediana": 14.0},
        {"Data": "2026-06-19", "DataReferencia": "2027", "Mediana": 12.0},
        {"Data": "2026-06-12", "DataReferencia": "2026", "Mediana": 14.25},
        {"Data": "2026-06-12", "DataReferencia": "2028", "Mediana": 11.0},
    ]
    captured = _capture(monkeypatch, rows)
    s = _series({"resource": "ExpectativasMercadoAnuais", "indicador": "Selic", "horizon": "current_year"})
    bacen_focus.ingest_bacen_focus(s)
    df = captured["df"].sort("date")
    # only the forecast whose reference year == survey-date year survives
    assert df.height == 2
    assert df["value"].to_list() == [14.25, 14.0]


def test_rolling_horizon_takes_value_as_is(monkeypatch):
    rows = [{"Data": "2026-06-19", "Mediana": 4.1}, {"Data": "2026-06-12", "Mediana": 4.2}]
    captured = _capture(monkeypatch, rows)
    s = _series({"resource": "ExpectativasMercadoInflacao12Meses", "indicador": "IPCA"})
    bacen_focus.ingest_bacen_focus(s)
    assert set(captured["df"]["value"].to_list()) == {4.1, 4.2}


def test_missing_extra_raises():
    with pytest.raises(ValueError):
        bacen_focus.ingest_bacen_focus(_series({"indicador": "IPCA"}))  # no resource
