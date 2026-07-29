"""Event-time algebra for the streaming lane — tumbling windows and watermarks.

Spark owns the real windowing and the real watermark; this module is a faithful,
**JVM-free reimplementation of the same arithmetic**, for two reasons:

1. **It is testable.** Window boundary math and "is this event late?" are exactly
   the semantics that are easy to get wrong and expensive to get wrong silently.
   A unit test that needs a Spark session to assert that 12:01:00.000 belongs to
   the *next* window is a test nobody runs.
2. **The job uses it.** :func:`ofl.streaming.silver.run_silver_stream` validates
   its arguments with :func:`parse_duration` and, at stop time, reports which
   windows are still held open in state with :func:`open_windows` — that report
   is the evidence in ``docs/STREAMING.md``.

The rules being mirrored, all of which are Spark's, not ours:

* A tumbling window of width *w* over timestamp *t* is ``[floor(t/w)*w, +w)``,
  floored on the **epoch** (Spark's ``window()`` with no ``startTime`` offset).
  A timestamp that lands exactly on a boundary belongs to the **later** window.
* The watermark after seeing a maximum event time *m* with delay *d* is
  ``m - d``. It only ever moves **forward**, and within a micro-batch Spark uses
  the watermark computed from the *previous* batch — so the tail of a stream is
  always still in state when the query stops.
* A record is **late** (dropped by a stateful operator) when its event time is
  **strictly before** the watermark. ``event_time == watermark`` survives.
* A window is **closed** (emitted, in append mode) when the watermark reaches its
  **end**, i.e. ``watermark >= window_end``.
"""

from __future__ import annotations

from datetime import datetime, timedelta

#: Units accepted by Spark's interval strings that make sense for this lane.
_UNITS = {
    "second": 1,
    "seconds": 1,
    "minute": 60,
    "minutes": 60,
    "hour": 3600,
    "hours": 3600,
}

_MICROS = 1_000_000


def parse_duration(text: str) -> timedelta:
    """Parse a Spark-style interval string (``"1 minute"``, ``"30 seconds"``).

    Deliberately a small, strict subset: the window and watermark arguments are
    handed to Spark verbatim, so anything this function accepts must mean the same
    thing to Spark. Rejecting ``"1 day"`` here is better than silently building a
    day-wide window because a caller typed the wrong unit.

    Raises:
        ValueError: on an unparseable string or a non-positive duration.
    """
    parts = str(text).strip().split()
    if len(parts) != 2:
        raise ValueError(f"expected '<n> <unit>', got {text!r}")
    amount, unit = parts
    if unit not in _UNITS:
        raise ValueError(f"unsupported interval unit {unit!r}; use {sorted(_UNITS)}")
    try:
        value = int(amount)
    except ValueError:
        raise ValueError(f"interval amount must be a whole number, got {amount!r}") from None
    if value <= 0:
        raise ValueError(f"interval must be positive, got {text!r}")
    return timedelta(seconds=value * _UNITS[unit])


def window_bounds(ts: datetime, window: str | timedelta) -> tuple[datetime, datetime]:
    """Tumbling window ``[start, end)`` containing ``ts``.

    Floored on the epoch, in **microseconds** — Spark timestamps are microsecond
    precision, and doing this in floats loses the very boundary the test is about.
    """
    width = window if isinstance(window, timedelta) else parse_duration(window)
    width_us = _to_micros(width)
    epoch = datetime(1970, 1, 1, tzinfo=ts.tzinfo)
    offset_us = _to_micros(ts - epoch)
    # Floor division, so pre-epoch timestamps floor downward rather than toward zero.
    start_us = (offset_us // width_us) * width_us
    start = epoch + timedelta(microseconds=start_us)
    return start, start + width


def watermark_for(max_event_time: datetime, delay: str | timedelta) -> datetime:
    """Watermark implied by the largest event time seen so far."""
    d = delay if isinstance(delay, timedelta) else parse_duration(delay)
    return max_event_time - d


def is_late(event_time: datetime, watermark: datetime) -> bool:
    """True when a stateful operator would drop this record as late.

    Strictly-before: an event landing exactly *on* the watermark is still accepted,
    which is why a watermark of ``0 seconds`` is not the same as dropping nothing.
    """
    return event_time < watermark


def is_window_closed(window_end: datetime, watermark: datetime) -> bool:
    """True when append mode would have emitted this window and freed its state."""
    return watermark >= window_end


def open_windows(
    max_event_time: datetime, *, window: str | timedelta, delay: str | timedelta
) -> list[tuple[datetime, datetime]]:
    """Windows still held in state once the stream has seen ``max_event_time``.

    These are the windows an append-mode query has *not* emitted yet: their end is
    beyond the watermark, so more data could still legitimately arrive for them.
    This is why a bounded run over a finite source always leaves a tail in state,
    and why the silver row count is short of the window count by exactly this much.
    """
    width = window if isinstance(window, timedelta) else parse_duration(window)
    return _open_from(watermark_for(max_event_time, delay), max_event_time, width)


def open_windows_at(
    watermark: datetime, *, window: str | timedelta, delay: str | timedelta
) -> list[tuple[datetime, datetime]]:
    """Same, but stated in terms of the **watermark** rather than the newest event.

    This is the form a job can report after a *resumed* run. A checkpoint persists
    the watermark, not the event times that produced it, so the largest event time
    seen *this run* says nothing about how far the query as a whole has advanced —
    a run that only ingests late data has a max event time far behind its own
    watermark. Since the watermark is by definition ``newest - delay``, the newest
    event the query has seen is recoverable from it.
    """
    width = window if isinstance(window, timedelta) else parse_duration(window)
    d = delay if isinstance(delay, timedelta) else parse_duration(delay)
    return _open_from(watermark, watermark + d, width)


def _open_from(
    watermark: datetime, newest: datetime, width: timedelta
) -> list[tuple[datetime, datetime]]:
    out: list[tuple[datetime, datetime]] = []
    start, _ = window_bounds(newest, width)
    # Walk backwards from the newest window while the watermark has not reached it.
    while not is_window_closed(start + width, watermark):
        out.append((start, start + width))
        start -= width
    return list(reversed(out))


def _to_micros(delta: timedelta) -> int:
    return (delta.days * 86_400 + delta.seconds) * _MICROS + delta.microseconds
