"""M2 semantics: tumbling-window boundary math and watermark/late-event handling.

These mirror Spark's rules (see ``ofl.streaming.windows``) and run without a JVM,
so the semantics that are easy to get subtly wrong stay covered by the fast suite.
"""

from datetime import datetime, timedelta

import pytest

from ofl.streaming.windows import (
    is_late,
    is_window_closed,
    open_windows,
    open_windows_at,
    parse_duration,
    watermark_for,
    window_bounds,
)

MINUTE = "1 minute"


def ts(text: str) -> datetime:
    return datetime.fromisoformat(text)


# --- interval parsing -------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("1 minute", 60), ("2 minutes", 120), ("30 seconds", 30), ("1 hour", 3600)],
)
def test_parse_duration_matches_sparks_interval_strings(text, seconds):
    assert parse_duration(text) == timedelta(seconds=seconds)


@pytest.mark.parametrize("text", ["1 day", "minute", "1", "0 seconds", "-5 minutes", "1.5 minutes"])
def test_parse_duration_rejects_what_spark_would_read_differently(text):
    # Better to fail at argument-parse time than to silently build a day-wide bar.
    with pytest.raises(ValueError):
        parse_duration(text)


# --- window boundary math ---------------------------------------------------


def test_window_contains_its_start_and_excludes_its_end():
    start, end = window_bounds(ts("2026-07-29T00:11:00"), MINUTE)
    assert (start, end) == (ts("2026-07-29T00:11:00"), ts("2026-07-29T00:12:00"))


def test_timestamp_on_a_boundary_belongs_to_the_later_window():
    # The half-open interval is the whole point: 00:12:00.000 is the *next* bar.
    assert window_bounds(ts("2026-07-29T00:12:00"), MINUTE)[0] == ts("2026-07-29T00:12:00")
    assert window_bounds(ts("2026-07-29T00:11:59.999999"), MINUTE)[0] == ts("2026-07-29T00:11:00")


def test_window_math_is_exact_at_microsecond_precision():
    # Spark timestamps are microseconds; doing this in floats loses the boundary.
    just_under = ts("2026-07-29T00:11:00") - timedelta(microseconds=1)
    assert window_bounds(just_under, MINUTE)[0] == ts("2026-07-29T00:10:00")


def test_windows_are_floored_on_the_epoch_not_on_the_first_event():
    # No startTime offset: the grid is absolute, so two runs bucket identically.
    assert window_bounds(ts("2026-07-29T00:11:37.421"), MINUTE)[0] == ts("2026-07-29T00:11:00")
    assert window_bounds(ts("1970-01-01T00:00:00"), MINUTE)[0] == ts("1970-01-01T00:00:00")


def test_pre_epoch_timestamps_floor_downward():
    # Floor division, not truncation toward zero — otherwise the bar jumps forward.
    assert window_bounds(ts("1969-12-31T23:59:30"), MINUTE)[0] == ts("1969-12-31T23:59:00")


def test_wider_windows_use_the_same_grid():
    assert window_bounds(ts("2026-07-29T00:11:37"), "5 minutes")[0] == ts("2026-07-29T00:10:00")
    assert window_bounds(ts("2026-07-29T00:11:37"), "1 hour") == (
        ts("2026-07-29T00:00:00"),
        ts("2026-07-29T01:00:00"),
    )


def test_a_window_is_exactly_as_wide_as_its_interval():
    start, end = window_bounds(ts("2026-07-29T00:11:37"), "30 seconds")
    assert end - start == timedelta(seconds=30)
    assert start == ts("2026-07-29T00:11:30")


# --- watermark / late events ------------------------------------------------


def test_watermark_trails_the_largest_event_time_by_the_delay():
    assert watermark_for(ts("2026-07-29T00:14:20"), "2 minutes") == ts("2026-07-29T00:12:20")


def test_event_before_the_watermark_is_late():
    watermark = ts("2026-07-29T00:12:20")
    assert is_late(ts("2026-07-29T00:12:19.999999"), watermark)
    assert not is_late(ts("2026-07-29T00:12:21"), watermark)


def test_an_event_exactly_on_the_watermark_survives():
    # Strictly-before, so a zero-delay watermark still admits on-time events.
    watermark = ts("2026-07-29T00:12:20")
    assert not is_late(watermark, watermark)


def test_a_late_event_is_only_late_relative_to_how_far_the_stream_has_advanced():
    late_trade = ts("2026-07-29T00:05:00")
    # Early in the run the same event is perfectly on time...
    assert not is_late(late_trade, watermark_for(ts("2026-07-29T00:06:00"), "2 minutes"))
    # ...and after the stream has moved on, the identical event is dropped.
    assert is_late(late_trade, watermark_for(ts("2026-07-29T00:14:20"), "2 minutes"))


def test_window_closes_only_once_the_watermark_reaches_its_end():
    _, end = window_bounds(ts("2026-07-29T00:11:30"), MINUTE)  # ends 00:12:00
    assert not is_window_closed(end, ts("2026-07-29T00:11:59.999999"))
    assert is_window_closed(end, end)  # watermark == window_end emits it


def test_a_bar_survives_late_data_up_to_the_watermark():
    # The whole point of allowed lateness: a trade for the 00:11 bar arriving while
    # the stream is at 00:12:30 still lands in 00:11, because that bar is still open.
    bar_start, bar_end = window_bounds(ts("2026-07-29T00:11:30"), MINUTE)
    watermark = watermark_for(ts("2026-07-29T00:12:30"), "2 minutes")  # 00:10:30
    assert not is_late(ts("2026-07-29T00:11:30"), watermark)
    assert not is_window_closed(bar_end, watermark)
    assert bar_start == ts("2026-07-29T00:11:00")


# --- what is still in state when the query stops ----------------------------


def test_open_windows_are_the_tail_the_watermark_has_not_passed():
    # Max event time 00:14:20 with a 2-minute delay puts the watermark at 00:12:20,
    # so 00:12, 00:13 and 00:14 are still open and unemitted.
    assert [w[0] for w in open_windows(ts("2026-07-29T00:14:20"), window=MINUTE, delay="2 minutes")] == [
        ts("2026-07-29T00:12:00"),
        ts("2026-07-29T00:13:00"),
        ts("2026-07-29T00:14:00"),
    ]


def test_every_open_window_ends_after_the_watermark():
    max_event = ts("2026-07-29T00:14:20")
    watermark = watermark_for(max_event, "2 minutes")
    for _, end in open_windows(max_event, window=MINUTE, delay="2 minutes"):
        assert not is_window_closed(end, watermark)


def test_a_zero_length_tail_still_leaves_the_current_window_open():
    # Even with no allowed lateness the newest bar is unfinished: the watermark sits
    # inside it, not at its end. A bounded run therefore always retains some state.
    assert open_windows(ts("2026-07-29T00:14:20"), window=MINUTE, delay="1 second") == [
        (ts("2026-07-29T00:14:00"), ts("2026-07-29T00:15:00"))
    ]


def test_open_windows_agree_whether_stated_from_the_event_or_the_watermark():
    max_event = ts("2026-07-29T00:14:20")
    mark = watermark_for(max_event, "2 minutes")
    assert open_windows_at(mark, window=MINUTE, delay="2 minutes") == open_windows(
        max_event, window=MINUTE, delay="2 minutes"
    )


def test_open_windows_from_the_watermark_ignore_a_late_only_run():
    # A resumed run that ingests nothing but late data has a max event time behind
    # its own watermark. Deriving the open windows from the run's events would claim
    # already-emitted bars are still open; the checkpointed watermark does not lie.
    mark = ts("2026-07-29T00:12:20")
    from_watermark = open_windows_at(mark, window=MINUTE, delay="2 minutes")
    misleading = open_windows(ts("2026-07-29T00:11:45"), window=MINUTE, delay="2 minutes")
    assert [w[0] for w in from_watermark] == [
        ts("2026-07-29T00:12:00"),
        ts("2026-07-29T00:13:00"),
        ts("2026-07-29T00:14:00"),
    ]
    assert misleading[0][0] < from_watermark[0][0]


def test_a_longer_watermark_holds_more_bars_in_state():
    max_event = ts("2026-07-29T00:14:20")
    short = open_windows(max_event, window=MINUTE, delay="2 minutes")
    long = open_windows(max_event, window=MINUTE, delay="10 minutes")
    assert len(long) > len(short)
    # State cost is linear in the watermark — that is the trade being made.
    assert len(long) - len(short) == 8
