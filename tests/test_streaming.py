import json

from ofl.streaming.producer import LandingWriter, unwrap_frame, ws_url
from ofl.streaming.schema import (
    REQUIRED_FIELDS,
    TRADE_EVENT_DDL,
    ddl_field_names,
    dedup_key,
)

# A frame captured verbatim off wss://stream.binance.com:9443/stream — the schema
# is asserted against real wire output rather than against our idea of it.
_FRAME = {
    "stream": "ethusdt@trade",
    "data": {
        "e": "trade",
        "E": 1785282999517,
        "s": "ETHUSDT",
        "t": 4241837850,
        "p": "1920.67000000",
        "q": "0.00270000",
        "T": 1785282999517,
        "m": False,
        "M": True,
    },
}


def test_ddl_covers_every_wire_field():
    assert set(ddl_field_names()) == set(_FRAME["data"])


def test_ddl_declares_no_duplicate_fields():
    names = ddl_field_names()
    assert len(names) == len(set(names))


def test_required_fields_are_declared_in_the_ddl():
    assert set(REQUIRED_FIELDS) <= set(ddl_field_names(TRADE_EVENT_DDL))


def test_dedup_key_carries_symbol_and_trade_id():
    # A trade id is unique only within a symbol, so the key must include both.
    assert dedup_key("ETHUSDT", 4241837850) == "ETHUSDT:4241837850"
    assert dedup_key("BTCUSDT", 1) != dedup_key("ETHUSDT", 1)


def test_dedup_key_is_stable_across_int_and_str_ids():
    assert dedup_key("ETHUSDT", 42) == dedup_key("ETHUSDT", "42")


def test_unwrap_returns_the_inner_payload():
    assert unwrap_frame(json.dumps(_FRAME)) == _FRAME["data"]


def test_unwrap_accepts_a_non_combined_frame():
    assert unwrap_frame(json.dumps(_FRAME["data"])) == _FRAME["data"]


def test_unwrap_rejects_control_and_malformed_frames():
    assert unwrap_frame('{"result":null,"id":1}') is None  # subscription ack
    assert unwrap_frame('{"data":{"e":"depthUpdate","s":"ETHUSDT"}}') is None
    assert unwrap_frame("{not json") is None
    assert unwrap_frame("[]") is None


def test_flush_is_atomic_and_leaves_no_partial_file(tmp_path):
    # Spark lists the landing dir and reads whatever it finds, so a file may only
    # ever become visible complete. The writer stages elsewhere and renames.
    landing, staging = tmp_path / "_landing", tmp_path / "_tmp"
    writer = LandingWriter(landing, staging)
    for _ in range(3):
        writer.add(_FRAME["data"])

    written = writer.flush()

    assert written is not None and written.parent == landing
    assert list(staging.iterdir()) == []
    lines = written.read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(line) for line in lines] == [_FRAME["data"]] * 3


def test_flush_on_empty_buffer_writes_nothing(tmp_path):
    writer = LandingWriter(tmp_path / "_landing", tmp_path / "_tmp")
    assert writer.flush() is None
    assert list((tmp_path / "_landing").iterdir()) == []


def test_each_flush_lands_a_distinct_file(tmp_path):
    writer = LandingWriter(tmp_path / "_landing", tmp_path / "_tmp")
    writer.add(_FRAME["data"])
    first = writer.flush()
    writer.add(_FRAME["data"])
    second = writer.flush()

    assert first != second
    assert (writer.files_written, writer.events_written) == (2, 2)


def test_ws_url_builds_a_combined_trade_stream():
    url = ws_url(["BTCUSDT", "ethusdt"])
    assert url.endswith("?streams=btcusdt@trade/ethusdt@trade")
    assert url.startswith("wss://")
