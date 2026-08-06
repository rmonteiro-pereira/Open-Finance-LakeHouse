"""Brazilian business-day calendar — pure Polars, no Spark, no cluster.

``build_dim_date`` used to compute the whole calendar inside a Spark session, which put
it out of reach of the test suite (the CI installs ``.[dev]`` only — no ``spark`` extra).
Every base-252 statement in the lakehouse depends on this calendar being right:
accumulated SELIC, the "D+1 business day" freshness budget, and the completeness check
that decides whether an ex-post window may be published at all.

So the calendar is computed here, in Polars, and ``build_dim_date`` is a thin shell that
writes the frame. The logic is testable; the Spark call is not asked to be.

The holiday input is versioned at ``ofl/reference/holidays_br.yml`` — read its header for
what is and is not in scope.
"""

from __future__ import annotations

import datetime as dt
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    import polars as pl

_HOLIDAYS_PATH = Path(__file__).resolve().parent / "reference" / "holidays_br.yml"


@lru_cache
def _holiday_spec() -> dict[str, Any]:
    return yaml.safe_load(_HOLIDAYS_PATH.read_text(encoding="utf-8"))


def easter(year: int) -> dt.date:
    """Gregorian Easter Sunday (Meeus/Jones/Butcher, the anonymous algorithm).

    Used only as the anchor for the four movable market holidays. Kept explicit rather
    than pulled from a dependency because it is nine lines and it is the single point
    the whole movable calendar hangs from.
    """
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month, day = divmod(h + ell - 7 * m + 114, 31)
    return dt.date(year, month, day + 1)


@lru_cache
def brazilian_holidays(year: int) -> dict[dt.date, str]:
    """National holidays for ``year``, keyed by date.

    A fixed holiday with ``from_year`` is absent before that year — see the note on
    Consciência Negra in the YAML. Movable holidays are Easter plus an offset.
    """
    spec = _holiday_spec()
    out: dict[dt.date, str] = {}

    for h in spec["fixed"]:
        if year < h.get("from_year", 0):
            continue
        out[dt.date(year, h["month"], h["day"])] = h["name"]

    anchor = easter(year)
    for h in spec["movable"]:
        out[anchor + dt.timedelta(days=h["offset_days"])] = h["name"]

    return out


def _holidays_between(start: dt.date, end: dt.date) -> dict[dt.date, str]:
    # A movable holiday computed from year Y always lands in Y, so iterating the span's
    # years is sufficient; the dict comprehension then clips to the requested window.
    merged: dict[dt.date, str] = {}
    for year in range(start.year, end.year + 1):
        merged.update(brazilian_holidays(year))
    return {d: name for d, name in merged.items() if start <= d <= end}


def build_calendar(start: str | dt.date = "1980-01-01", end: str | dt.date = "2035-12-31") -> "pl.DataFrame":
    """The ``dim_date`` frame: one row per calendar day over ``[start, end]``.

    ``day_of_week`` keeps Spark's ``dayofweek`` convention (1 = Sunday … 7 = Saturday)
    so the column does not change meaning for anything already reading ``dim_date``.
    """
    import polars as pl

    start_d = dt.date.fromisoformat(start) if isinstance(start, str) else start
    end_d = dt.date.fromisoformat(end) if isinstance(end, str) else end
    if end_d < start_d:
        raise ValueError(f"end {end_d} precedes start {start_d}")

    holidays = _holidays_between(start_d, end_d)

    df = pl.DataFrame({"date": pl.date_range(start_d, end_d, "1d", eager=True)})
    return df.with_columns(
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32).alias("date_key"),
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.quarter().alias("quarter"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.day().alias("day"),
        # Polars weekday() is 1 = Monday … 7 = Sunday; Spark's dayofweek is
        # 1 = Sunday … 7 = Saturday. Convert so the column keeps its published meaning.
        ((pl.col("date").dt.weekday() % 7) + 1).cast(pl.Int32).alias("day_of_week"),
        (pl.col("date").dt.month_end() == pl.col("date")).alias("is_month_end"),
        pl.col("date").replace_strict(holidays, default=None).alias("holiday_name"),
    ).with_columns(
        (
            pl.col("date").dt.weekday().is_between(1, 5)  # Mon–Fri
            & pl.col("holiday_name").is_null()
        ).alias("is_business_day_br")
    )


def business_days_between(start: dt.date, end: dt.date) -> int:
    """Count business days in ``[start, end]`` — the expected-window denominator.

    This is what makes an ex-post window falsifiable: a published window declares how
    many business days it *should* contain, so a gap in the daily series is visible
    instead of being silently compounded away.
    """
    return int(build_calendar(start, end)["is_business_day_br"].sum())
