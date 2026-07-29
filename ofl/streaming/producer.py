"""M0 — producer: Binance public trade WebSocket → append-only JSONL landing files.

The feed is free, needs no key and matches the lakehouse's financial theme, so it
stands in for a market-data tap. Frames are unwrapped from the combined-stream
envelope and written **verbatim**: bronze keeps the source's shape, and any
normalisation is the Spark job's job.

Two properties matter for the reader downstream:

* **Atomic visibility.** Each flush is built in ``_landing_tmp`` and renamed into
  ``_landing`` with ``os.replace``. Spark's file source lists a directory and reads
  whatever it finds, so a partially written file would be read as truncated JSON —
  the rename makes each file appear complete or not at all.
* **A bounded run.** The producer always stops: on ``--max-seconds``,
  ``--max-events``, or Ctrl-C, flushing what it holds on the way out. Nothing here
  is meant to be left running unattended.

Usage::

    ofl stream-produce --symbols btcusdt,ethusdt --max-seconds 120
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from ofl.platform.logging import get_logger
from ofl.streaming.paths import landing_dir, landing_tmp_dir
from ofl.streaming.schema import TRADE_EVENT_TYPE

log = get_logger(__name__)

_WS_BASE = "wss://stream.binance.com:9443/stream"
_DEFAULT_SYMBOLS = ("btcusdt", "ethusdt", "solusdt")

# Reconnect backoff, seconds. The public endpoint drops idle/long-lived sockets
# (and Binance recycles them every 24h by design), so a drop is normal operation,
# not an error — resume rather than fail the run.
_BACKOFF = (1.0, 2.0, 5.0, 10.0)


def ws_url(symbols: list[str] | tuple[str, ...] = _DEFAULT_SYMBOLS) -> str:
    """Combined-stream URL for the ``@trade`` channel of each symbol."""
    if not symbols:
        raise ValueError("at least one symbol is required")
    streams = "/".join(f"{s.strip().lower()}@trade" for s in symbols)
    return f"{_WS_BASE}?streams={streams}"


def unwrap_frame(raw: str | bytes) -> dict | None:
    """Extract the trade payload from a combined-stream frame.

    Returns ``None`` for anything that is not a trade event — subscription acks,
    control frames, and malformed JSON. Those are counted, not landed: the
    dead-letter table is for records that *claim* to be data and fail the schema,
    not for protocol chatter.
    """
    try:
        frame = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(frame, dict):
        return None
    payload = frame.get("data", frame)  # tolerate a raw (non-combined) stream too
    if not isinstance(payload, dict) or payload.get("e") != TRADE_EVENT_TYPE:
        return None
    return payload


class LandingWriter:
    """Buffers events and flushes them as one atomically-renamed JSONL file."""

    def __init__(self, landing: Path, tmp: Path, *, prefix: str = "trades") -> None:
        self.landing = landing
        self.tmp = tmp
        self.prefix = prefix
        self.landing.mkdir(parents=True, exist_ok=True)
        self.tmp.mkdir(parents=True, exist_ok=True)
        self._buf: list[str] = []
        self._seq = 0
        self.files_written = 0
        self.events_written = 0

    def add(self, event: dict) -> None:
        self._buf.append(json.dumps(event, separators=(",", ":"), sort_keys=True))

    def __len__(self) -> int:
        return len(self._buf)

    def flush(self) -> Path | None:
        """Write the buffer to ``_landing`` atomically. No-op when empty."""
        if not self._buf:
            return None
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        name = f"{self.prefix}-{stamp}-{self._seq:05d}.jsonl"
        staged = self.tmp / name
        staged.write_text("\n".join(self._buf) + "\n", encoding="utf-8")
        final = self.landing / name
        os.replace(staged, final)  # atomic within a filesystem: Spark sees all or nothing

        self._seq += 1
        self.files_written += 1
        self.events_written += len(self._buf)
        log.info("landing_flush", file=final.name, events=len(self._buf))
        self._buf.clear()
        return final


async def _consume(
    writer: LandingWriter,
    url: str,
    *,
    deadline: float,
    max_events: int,
    flush_events: int,
    flush_seconds: float,
) -> dict:
    """Read frames until a cap is hit, flushing on size or age. Reconnects on drop."""
    import websockets

    seen = skipped = 0
    attempt = 0
    last_flush = time.monotonic()

    while time.monotonic() < deadline and seen < max_events:
        try:
            # close_timeout: the graceful-close handshake otherwise blocks the exit
            # for its 10s default, overshooting the run cap for no benefit here.
            async with websockets.connect(
                url, open_timeout=15, ping_interval=20, close_timeout=3
            ) as ws:
                attempt = 0
                log.info("ws_connected", url=url)
                while time.monotonic() < deadline and seen < max_events:
                    timeout = min(deadline - time.monotonic(), flush_seconds)
                    if timeout <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except TimeoutError:
                        raw = None  # quiet market: fall through to the age-based flush
                    if raw is not None:
                        event = unwrap_frame(raw)
                        if event is None:
                            skipped += 1
                        else:
                            writer.add(event)
                            seen += 1
                    aged = time.monotonic() - last_flush >= flush_seconds
                    if len(writer) >= flush_events or (writer and aged):
                        writer.flush()
                        last_flush = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any transport fault is retryable
            if time.monotonic() >= deadline or seen >= max_events:
                break
            wait = _BACKOFF[min(attempt, len(_BACKOFF) - 1)]
            attempt += 1
            log.warning("ws_reconnect", error=str(exc), attempt=attempt, sleep=wait)
            await asyncio.sleep(min(wait, max(0.0, deadline - time.monotonic())))

    writer.flush()  # never lose the tail of a run
    return {"events": seen, "skipped": skipped}


def run_producer(
    symbols: list[str] | None = None,
    *,
    max_seconds: float = 120.0,
    max_events: int = 20_000,
    flush_events: int = 500,
    flush_seconds: float = 5.0,
) -> dict:
    """Capture live trades to the landing directory. Always terminates.

    Args:
        symbols: exchange symbols, e.g. ``["btcusdt", "ethusdt"]``.
        max_seconds: wall-clock cap on the run.
        max_events: cap on landed events.
        flush_events: flush once this many events are buffered.
        flush_seconds: flush at least this often, so a quiet feed still produces
            files for the streaming job to pick up.
    """
    url = ws_url(symbols or list(_DEFAULT_SYMBOLS))
    writer = LandingWriter(landing_dir(), landing_tmp_dir())
    deadline = time.monotonic() + max_seconds

    log.info(
        "producer_start",
        url=url,
        landing=str(writer.landing),
        max_seconds=max_seconds,
        max_events=max_events,
    )
    started = time.monotonic()
    try:
        result = asyncio.run(
            _consume(
                writer,
                url,
                deadline=deadline,
                max_events=max_events,
                flush_events=flush_events,
                flush_seconds=flush_seconds,
            )
        )
    except KeyboardInterrupt:  # Ctrl-C is a normal stop, not a failure
        with contextlib.suppress(Exception):
            writer.flush()
        result = {"events": writer.events_written, "skipped": 0, "interrupted": True}

    summary = {
        **result,
        "files": writer.files_written,
        "landed": writer.events_written,
        "seconds": round(time.monotonic() - started, 1),
        "landing": str(writer.landing),
    }
    log.info("producer_done", **summary)
    return summary
