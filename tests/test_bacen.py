"""Unit tests for the BACEN SGS window walk (``ofl.ingestion.bacen``).

The walk is pure control flow around ``_get_window``, so every test stubs that
one seam and drives ``fetch_sgs`` offline — same convention as the other
extractor tests.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from ofl.ingestion import bacen
from ofl.ingestion.bacen import fetch_sgs


def _payload(rows: list[tuple[str, str]]) -> list[dict]:
    return [{"data": d, "valor": v} for d, v in rows]


class _WindowStub:
    """Record every requested window and answer from a canned script."""

    def __init__(self, script):
        self.script = list(script)
        self.calls: list[tuple[date, date]] = []

    def __call__(self, series_id: int, start: date, end: date):
        self.calls.append((start, end))
        if not self.script:
            return "empty", None
        return self.script.pop(0)


def test_walk_survives_leap_day_end(monkeypatch):
    """A walk anchored on Feb 29 must not crash when the window start lands in a
    non-leap year.

    ``fetch_sgs`` defaults ``end`` to ``date.today()``: on 2024-02-29 the first
    window start was ``end.replace(year=2015)`` — a ``ValueError``, which took
    down every ``bacen_sgs`` series scheduled that day. The walk must clamp to
    Feb 28 instead of raising.
    """
    stub = _WindowStub([("ok", _payload([("29/02/2024", "13.65")]))])
    monkeypatch.setattr(bacen, "_get_window", stub)

    # floor year 2019 is not a leap year, so the first window start is
    # "2019-02-29" — the exact shape that used to raise ValueError.
    out = fetch_sgs(1, end=date(2024, 2, 29), since=date(2019, 1, 1))

    assert out.height == 1
    assert out.row(0, named=True) == {"date": date(2024, 2, 29), "value": 13.65}
    # The clamped window must still be sane: start <= end, floored at `since`.
    for start, end in stub.calls:
        assert start <= end
        assert start >= date(2019, 1, 1)


def test_walk_dedups_overlapping_windows(monkeypatch):
    """SGS windows overlap at the seams; the same (date, value) row landing in
    two windows must survive as exactly one output row."""
    stub = _WindowStub(
        [
            ("ok", _payload([("02/01/2020", "2.0"), ("03/01/2020", "3.0")])),
            ("ok", _payload([("01/01/2015", "1.0"), ("02/01/2020", "2.0")])),
        ]
    )
    monkeypatch.setattr(bacen, "_get_window", stub)

    out = fetch_sgs(1, end=date(2024, 6, 30), since=date(2010, 1, 1))

    assert out["date"].to_list() == [date(2015, 1, 1), date(2020, 1, 2), date(2020, 1, 3)]
    assert out["date"].n_unique() == out.height


def test_walk_stops_after_inception(monkeypatch):
    """Once data has been seen, the first empty window means the walk has passed
    the series' first observation — it must stop, not scan back to 1900."""
    stub = _WindowStub(
        [
            ("ok", _payload([("01/06/2019", "1.0")])),
            ("empty", None),
            # Anything after this would be a bug; the stub would keep answering
            # "empty" and the call count below would grow.
        ]
    )
    monkeypatch.setattr(bacen, "_get_window", stub)

    out = fetch_sgs(1, end=date(2024, 6, 30))

    assert out.height == 1
    assert len(stub.calls) == 2


def test_walk_returns_empty_frame_when_no_data(monkeypatch):
    monkeypatch.setattr(bacen, "_get_window", _WindowStub([]))
    out = fetch_sgs(1, end=date(2024, 6, 30), since=date(2023, 1, 1))
    assert out.height == 0
    assert out.schema == pl.Schema({"date": pl.Date, "value": pl.Float64})
