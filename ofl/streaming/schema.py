"""Explicit contract for the wire events — a stream is never schema-inferred.

Inferring a schema on a stream is unsafe: Spark would sample whatever files happen
to exist at start-up and then silently fail (or drift) when the feed sends anything
else, and the inferred types would not survive a restart from checkpoint. So the
wire format is pinned here as a DDL string and handed to ``from_json``; anything
that does not fit lands in the dead-letter table instead of corrupting bronze.

Wire format: Binance ``<symbol>@trade`` payload (the ``data`` member of a combined
stream frame). Field names are the exchange's, verbatim — bronze keeps raw shape
and only the projection to typed columns is ours.

.. warning::
   The exchange uses case to distinguish fields: ``E`` (event time) vs ``e`` (event
   type), ``T`` (trade time) vs ``t`` (trade id). Spark resolves struct fields
   case-insensitively by default, which makes those pairs ambiguous, so the
   streaming session sets ``spark.sql.caseSensitive=true``. See
   ``ofl.streaming.bronze``.
"""

from __future__ import annotations

#: Wire schema handed to ``from_json``. One entry per field the exchange sends.
TRADE_EVENT_DDL = (
    "e STRING, "  # event type, always "trade"
    "E LONG, "  # event time, epoch milliseconds
    "s STRING, "  # symbol, e.g. BTCUSDT
    "t LONG, "  # trade id, monotonic per symbol
    "p STRING, "  # price — sent as a decimal *string*, cast on the way to bronze
    "q STRING, "  # quantity — likewise a decimal string
    "T LONG, "  # trade time, epoch milliseconds
    "m BOOLEAN, "  # buyer is the market maker (i.e. the trade hit the bid)
    "M BOOLEAN"  # exchange-internal "ignore" flag, kept so the contract is total
)

#: Fields that must be present for a record to be admissible to bronze. Everything
#: else may be null; without these the event has no grain and no measure.
REQUIRED_FIELDS = ("s", "t", "p", "q", "T")

#: Value of the ``e`` discriminator for the events this lane consumes.
TRADE_EVENT_TYPE = "trade"

#: Logical source name recorded on every bronze row (matches the batch lanes'
#: ``source`` lineage column).
SOURCE = "binance_ws"


def ddl_field_names(ddl: str = TRADE_EVENT_DDL) -> tuple[str, ...]:
    """Field names declared by a flat DDL string, in order.

    Lets tests assert the contract against a captured payload without needing a
    JVM, and keeps the DDL the single definition of the wire format.
    """
    return tuple(part.strip().split()[0] for part in ddl.split(",") if part.strip())


def dedup_key(symbol: str, trade_id: int | str) -> str:
    """Idempotency key for a trade: ``SYMBOL:trade_id``.

    A trade id is only unique *within* a symbol, so the key has to carry both. This
    is what M2's ``dropDuplicatesWithinWatermark`` keys on and what a Delta ``MERGE``
    into silver matches on, which is why it is materialised in bronze rather than
    recomputed downstream.

    Kept as plain Python so it can be unit-tested; the Spark job builds the same
    string with :data:`DEDUP_KEY_SQL` over native columns.
    """
    return f"{symbol}:{trade_id}"


#: Spark expression producing the same value as :func:`dedup_key`, applied to the
#: parsed event struct. Tested against the Python implementation.
DEDUP_KEY_SQL = "concat_ws(':', evt.s, cast(evt.t as string))"
