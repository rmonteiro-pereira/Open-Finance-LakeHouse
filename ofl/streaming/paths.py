"""Canonical layout of the streaming lane under ``OFL_STREAMING_ROOT``.

::

    data/streaming/
      _landing/                    producer output, one JSONL file per flush
      _landing_tmp/                partial writes; renamed into _landing atomically
      _checkpoints/bronze_trades/  Spark offsets + commits (the exactly-once state)
      bronze/trades/               bronze Delta table (well-formed events)
      bronze/trades_dead_letter/   bronze Delta table (unparseable / incomplete)

Everything here is generated data and is gitignored. ``_landing_tmp`` is a sibling
of ``_landing`` rather than a child so that Spark's file source never sees a
half-written file: the producer builds each file in the tmp dir and then does a
single atomic rename into the directory Spark is watching.
"""

from __future__ import annotations

from pathlib import Path

from ofl.platform.io import streaming_dir

#: Directory the producer appends to and the Spark file source watches.
LANDING = ("_landing",)
#: Staging directory for in-progress files (renamed into ``LANDING`` on flush).
LANDING_TMP = ("_landing_tmp",)
#: Streaming checkpoint: offsets, commits and metadata. Deleting it replays.
CHECKPOINT = ("_checkpoints", "bronze_trades")
#: Bronze Delta table of well-formed trade events.
BRONZE_TRADES = ("bronze", "trades")
#: Bronze Delta table of records that failed the explicit schema.
DEAD_LETTER = ("bronze", "trades_dead_letter")


def landing_dir() -> Path:
    return streaming_dir(*LANDING)


def landing_tmp_dir() -> Path:
    return streaming_dir(*LANDING_TMP)


def checkpoint_dir() -> Path:
    return streaming_dir(*CHECKPOINT)


def bronze_trades_dir() -> Path:
    return streaming_dir(*BRONZE_TRADES)


def dead_letter_dir() -> Path:
    return streaming_dir(*DEAD_LETTER)
