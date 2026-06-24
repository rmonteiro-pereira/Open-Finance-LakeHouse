"""Unit tests for the B3 public-portal handler (arquivos.b3.com.br).

Parsers are tested against synthetic CSV bytes mirroring the real file headers
(``;``-separated, Latin-1, decimal comma, optional ``Status do Arquivo`` preamble).
Network I/O (the token handshake) is not exercised here.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from ofl.ingestion.b3_arquivos import (
    _DEFAULT_SEGMENTS,
    _resolve_window,
    parse_instruments,
    parse_open_position,
    parse_trade_information,
)

_OI = (
    "RptDt;TckrSymb;ISIN;Asst;XprtnCd;SgmtNm;OpnIntrst;VartnOpnIntrst;DstrbtnId;"
    "CvrdQty;TtlBlckdPos;UcvrdQty;TtlPos;BrrwrQty;LndrQty;CurQty;FwdPric\n"
    "2026-06-19;DI1F27;BRBMEFD1I4Z0;DI1;F27;FINANCIAL;6851297;82006;;;;;;;;;\n"
    "2026-06-19;CCMU26;BRBMEFCCMI11;CCM;U26;AGRIBUSINESS;1200;-30;;;;;;;;;\n"
).encode("latin-1")

_TRADE = (
    "Status do Arquivo: Final\n"
    "RptDt;TckrSymb;ISIN;SgmtNm;MinPric;MaxPric;TradAvrgPric;LastPric;OscnPctg;"
    "AdjstdQt;AdjstdQtTax;RefPric;TradQty;FinInstrmQty;NtlFinVol\n"
    "2026-06-19;DI1F27;BRBMEFD1I4Z0;FINANCIAL;14,225;14,265;14,249;14,255;0,06;"
    "93109,37;14,256;;9681;286656;26691234687,86\n"
).encode("latin-1")

# Only the columns the parser references need to be present.
_INST = (
    "Status do Arquivo: Final\n"
    "RptDt;TckrSymb;Asst;AsstDesc;SgmtNm;MktNm;SctyCtgyNm;XprtnDt;XprtnCd;ISIN;"
    "CFICd;OptnTp;CtrctMltplr;AllcnRndLot;TradgCcy;ExrcPric;OptnStyle;UndrlygTckrSymb1\n"
    "2026-06-19;DI1F27;DI1;DI1 JAN/27;FINANCIAL;FINANCIAL;;2027-01-04;F27;BRBMEFD1I4Z0;"
    "FUTSXX;;1;1;BRL;;;\n"
).encode("latin-1")


def test_parse_open_position():
    df = parse_open_position(_OI)
    assert df.height == 2
    di = df.filter(pl.col("symbol") == "DI1F27").row(0, named=True)
    assert di["open_interest"] == 6851297.0
    assert di["open_interest_var"] == 82006.0
    assert di["asset"] == "DI1" and di["segment"] == "FINANCIAL"
    assert di["date"] == date(2026, 6, 19)


def test_parse_trade_information_skips_preamble_and_decimal_comma():
    df = parse_trade_information(_TRADE)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["last_price"] == 14.255
    assert row["settlement_rate"] == 14.256  # AdjstdQtTax — the settlement rate
    assert row["contracts"] == 286656.0
    assert row["ref_price"] is None  # empty field -> null


def test_parse_instruments_dimension():
    df = parse_instruments(_INST)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["symbol"] == "DI1F27"
    assert row["expiration_date"] == date(2027, 1, 4)
    assert row["contract_multiplier"] == 1.0
    assert row["trading_ccy"] == "BRL"


def test_default_segments_are_futures_only():
    # Equity options are excluded by default (they ~25x the row count).
    assert _DEFAULT_SEGMENTS == ["FINANCIAL", "AGRIBUSINESS"]


def test_resolve_window_backfill_range_walks_weekdays():
    # 2026-06-19 is a Friday; the inclusive range to Mon 2026-06-22 skips the weekend.
    dates, is_backfill = _resolve_window({"start": "2026-06-19", "end": "2026-06-22"})
    assert is_backfill is True
    assert dates == [date(2026, 6, 19), date(2026, 6, 22)]


def test_resolve_window_default_lookback_is_not_backfill():
    dates, is_backfill = _resolve_window({"lookback_days": 3})
    assert is_backfill is False
    assert len(dates) == 3
    assert all(d.weekday() < 5 for d in dates)


def test_resolve_window_allow_env_false_ignores_backfill_override(monkeypatch):
    # The instrument snapshot must keep pulling a single latest day even when a
    # range backfill of the fact tables is configured via OFL_B3_WINDOW.
    monkeypatch.setenv("OFL_B3_WINDOW", "2026-06-01..2026-06-19")
    dates, is_backfill = _resolve_window({"lookback_days": 1}, allow_env=False)
    assert is_backfill is False
    assert len(dates) == 1
