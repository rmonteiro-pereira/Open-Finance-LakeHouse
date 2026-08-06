"""The Brazilian business-day calendar, against dates that are checkable by hand.

Every base-252 claim in the lakehouse rests on this file, so the assertions are known
holidays and structural invariants — never "whatever the code produced today".
"""

from datetime import date

import pytest

from ofl.calendar import (
    brazilian_holidays,
    build_calendar,
    business_days_between,
    easter,
)


@pytest.mark.parametrize(
    ("year", "expected"),
    [(2023, date(2023, 4, 9)), (2024, date(2024, 3, 31)), (2025, date(2025, 4, 20)), (2026, date(2026, 4, 5))],
)
def test_easter_matches_the_published_dates(year, expected):
    assert easter(year) == expected


@pytest.mark.parametrize(
    ("year", "day", "name_fragment"),
    [
        (2024, date(2024, 2, 12), "Carnaval"),
        (2024, date(2024, 2, 13), "Carnaval"),
        (2024, date(2024, 3, 29), "Sexta-feira Santa"),
        (2024, date(2024, 5, 30), "Corpus Christi"),
        (2025, date(2025, 3, 4), "Carnaval"),
        (2025, date(2025, 4, 18), "Sexta-feira Santa"),
        (2025, date(2025, 6, 19), "Corpus Christi"),
        (2026, date(2026, 2, 17), "Carnaval"),
        (2026, date(2026, 4, 3), "Sexta-feira Santa"),
        (2026, date(2026, 6, 4), "Corpus Christi"),
    ],
)
def test_movable_holidays_land_on_the_known_dates(year, day, name_fragment):
    holidays = brazilian_holidays(year)
    assert day in holidays
    assert name_fragment in holidays[day]


def test_consciencia_negra_is_national_only_from_2024():
    """Lei 14.759/2023. Treating it as national before 2024 deletes business days
    that existed, which would silently shorten every base-252 window in that history."""
    assert date(2023, 11, 20) not in brazilian_holidays(2023)
    assert date(2024, 11, 20) in brazilian_holidays(2024)
    assert date(2025, 11, 20) in brazilian_holidays(2025)


def test_fixed_holidays_are_present_every_year():
    holidays = brazilian_holidays(2025)
    for day in (
        date(2025, 1, 1),
        date(2025, 4, 21),
        date(2025, 5, 1),
        date(2025, 9, 7),
        date(2025, 10, 12),
        date(2025, 11, 2),
        date(2025, 11, 15),
        date(2025, 12, 25),
    ):
        assert day in holidays


def test_business_day_flag_is_weekday_and_not_holiday():
    """The structural invariant, checked over a decade rather than at sample points."""
    cal = build_calendar("2016-01-01", "2026-12-31")
    rows = cal.iter_rows(named=True)
    for row in rows:
        d = row["date"]
        holidays = brazilian_holidays(d.year)
        expected = d.weekday() < 5 and d not in holidays
        assert row["is_business_day_br"] == expected, d


def test_good_friday_and_tiradentes_are_not_business_days():
    cal = build_calendar("2025-04-14", "2025-04-22")
    by_date = {r["date"]: r for r in cal.iter_rows(named=True)}
    assert by_date[date(2025, 4, 18)]["is_business_day_br"] is False  # Sexta-feira Santa
    assert by_date[date(2025, 4, 19)]["is_business_day_br"] is False  # sábado
    assert by_date[date(2025, 4, 21)]["is_business_day_br"] is False  # Tiradentes, segunda
    assert by_date[date(2025, 4, 22)]["is_business_day_br"] is True


def test_day_of_week_keeps_the_spark_convention():
    """1 = Sunday .. 7 = Saturday, so the published column does not change meaning."""
    cal = build_calendar("2025-04-20", "2025-04-26")
    dow = {r["date"]: r["day_of_week"] for r in cal.iter_rows(named=True)}
    assert dow[date(2025, 4, 20)] == 1  # domingo
    assert dow[date(2025, 4, 21)] == 2  # segunda
    assert dow[date(2025, 4, 26)] == 7  # sábado


def test_month_end_flag():
    cal = build_calendar("2024-02-01", "2024-03-01")
    by_date = {r["date"]: r["is_month_end"] for r in cal.iter_rows(named=True)}
    assert by_date[date(2024, 2, 29)] is True  # bissexto
    assert by_date[date(2024, 2, 28)] is False


def test_calendar_covers_every_day_in_the_span():
    cal = build_calendar("2025-01-01", "2025-12-31")
    assert cal.height == 365
    assert cal["date_key"][0] == 20250101


def test_business_days_between_is_the_window_denominator():
    # 2025-04-14 (seg) .. 2025-04-22 (ter): 14,15,16,17 úteis; 18 Sexta-feira Santa;
    # 19-20 fim de semana; 21 Tiradentes; 22 útil. => 5
    assert business_days_between(date(2025, 4, 14), date(2025, 4, 22)) == 5


def test_reversed_span_is_an_error_not_an_empty_frame():
    """An empty window would make a completeness check pass vacuously."""
    with pytest.raises(ValueError):
        build_calendar("2025-12-31", "2025-01-01")
