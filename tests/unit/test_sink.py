from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pytest

pytest.importorskip("feast")

from pipeline.features.iceberg import TABLE_SCHEMA  # noqa: E402
from pipeline.features.sink import (  # noqa: E402
    PUSH_SOURCE,
    AppendBuffer,
    FeatureSink,
    parse_feature_message,
    to_arrow,
    to_push_frame,
    to_records,
)

INGESTED_AT = datetime(2026, 10, 5, 12, 1, 15, tzinfo=UTC)

# Exactly what the Spark job writes to market.features.1m.
FEATURE_JSON = {
    "symbol": "BTC-USD",
    "window_start": "2026-10-05T12:00:00.000Z",
    "window_end": "2026-10-05T12:01:00.000Z",
    "trade_count": 2,
    "volume": 4.0,
    "vwap": 107.5,
    "price_min": 100.0,
    "price_max": 110.0,
    "buy_volume": 1.0,
    "sell_volume": 3.0,
}


class FakeError:
    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code


class FakeMessage:
    def __init__(self, value: bytes, offset: int = 0, error: FakeError | None = None) -> None:
        self._value, self._offset, self._error = value, offset, error

    def value(self) -> bytes:
        return self._value

    def offset(self) -> int:
        return self._offset

    def error(self) -> FakeError | None:
        return self._error


class FakeConsumer:
    def __init__(self) -> None:
        self.commits = 0
        self.closed = False

    def commit(self, asynchronous: bool = True) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


class FakeStore:
    def __init__(self) -> None:
        self.pushes: list[tuple[str, Any]] = []

    def push(self, source: str, frame: Any, to: Any) -> None:
        self.pushes.append((source, frame))


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def message(**overrides: Any) -> FakeMessage:
    return FakeMessage(json.dumps({**FEATURE_JSON, **overrides}).encode())


def make_sink(
    append: Any = None, max_rows: int = 100, clock: FakeClock | None = None
) -> tuple[FeatureSink, FakeConsumer, FakeStore, list[pa.Table]]:
    consumer, store = FakeConsumer(), FakeStore()
    appended: list[pa.Table] = []
    sink = FeatureSink(
        consumer=consumer,  # type: ignore[arg-type]
        store=store,  # type: ignore[arg-type]
        append=append or appended.append,
        buffer=AppendBuffer(max_rows=max_rows, max_age_seconds=60, clock=clock or FakeClock()),
    )
    return sink, consumer, store, appended


# --- parsing and conversion ---------------------------------------------------------


def test_parses_spark_json_with_utc_timestamps() -> None:
    row = parse_feature_message(json.dumps(FEATURE_JSON))

    assert row.window_end == datetime(2026, 10, 5, 12, 1, tzinfo=UTC)
    assert row.vwap == 107.5


@pytest.mark.parametrize(
    "overrides",
    [
        {"vwap": -1.0},
        {"trade_count": 0},
        {"buy_volume": -0.5},
        {"window_end": "2026-10-05T12:01:00"},  # no time zone
        {"symbol": ""},
    ],
)
def test_rejects_invalid_feature_rows(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        parse_feature_message(json.dumps({**FEATURE_JSON, **overrides}))


def test_arrow_table_matches_iceberg_schema() -> None:
    records = to_records([parse_feature_message(json.dumps(FEATURE_JSON))], INGESTED_AT)

    table = to_arrow(records)

    assert table.schema.equals(TABLE_SCHEMA.as_arrow())
    assert table.column("ingested_at")[0].as_py() == INGESTED_AT


def test_push_frame_has_entity_and_tz_aware_timestamps() -> None:
    records = to_records([parse_feature_message(json.dumps(FEATURE_JSON))], INGESTED_AT)

    frame = to_push_frame(records)

    assert frame.loc[0, "symbol"] == "BTC-USD"
    assert str(frame["window_end"].dt.tz) == "UTC"


# --- buffering ------------------------------------------------------------------------


def test_buffer_flushes_on_row_count() -> None:
    buffer = AppendBuffer(max_rows=2, max_age_seconds=60, clock=FakeClock())

    buffer.add([{"a": 1}])
    assert not buffer.should_flush()
    buffer.add([{"a": 2}])
    assert buffer.should_flush()


def test_buffer_flushes_on_age_of_oldest_row() -> None:
    clock = FakeClock()
    buffer = AppendBuffer(max_rows=1000, max_age_seconds=60, clock=clock)

    buffer.add([{"a": 1}])
    clock.now = 59.9
    assert not buffer.should_flush()
    clock.now = 60.0
    assert buffer.should_flush()


def test_empty_buffer_never_flushes() -> None:
    clock = FakeClock()
    buffer = AppendBuffer(max_rows=1, max_age_seconds=1, clock=clock)
    clock.now = 100.0

    assert not buffer.should_flush()


# --- sink behaviour -------------------------------------------------------------------


def test_pushes_online_immediately_but_buffers_offline() -> None:
    sink, consumer, store, appended = make_sink()

    sink.process([message(), message(symbol="ETH-USD")])  # type: ignore[list-item]

    [(source, frame)] = store.pushes
    assert source == PUSH_SOURCE
    assert list(frame["symbol"]) == ["BTC-USD", "ETH-USD"]
    assert appended == []  # not flushed yet
    assert consumer.commits == 0  # nothing durable yet, so nothing committed


def test_flush_appends_then_commits_offsets() -> None:
    sink, consumer, _, appended = make_sink(max_rows=2)

    sink.process([message(), message(symbol="ETH-USD")])  # type: ignore[list-item]

    [table] = appended
    assert table.num_rows == 2
    assert consumer.commits == 1
    assert sink.stats.appended_offline == 2


def test_failed_append_keeps_rows_and_does_not_commit() -> None:
    """At-least-once: offsets are only committed after a durable Iceberg append."""

    def failing_append(table: pa.Table) -> None:
        raise OSError("storage unavailable")

    sink, consumer, _, _ = make_sink(append=failing_append, max_rows=1)

    sink.process([message()])  # type: ignore[list-item]

    assert consumer.commits == 0
    assert len(sink._buffer.records) == 1  # retried on the next flush


def test_invalid_messages_are_skipped_and_counted() -> None:
    sink, _, store, _ = make_sink()

    sink.process([FakeMessage(b"{not json"), message(vwap=-1.0), message()])  # type: ignore[list-item]

    assert sink.stats.received == 3
    assert sink.stats.invalid == 2
    [(_, frame)] = store.pushes
    assert len(frame) == 1


def test_partition_eof_is_not_counted_as_a_message() -> None:
    from confluent_kafka import KafkaError

    sink, _, store, _ = make_sink()

    sink.process([FakeMessage(b"", error=FakeError(KafkaError._PARTITION_EOF))])  # type: ignore[list-item]

    assert sink.stats.received == 0
    assert store.pushes == []


def test_close_flushes_remaining_rows() -> None:
    sink, consumer, _, appended = make_sink(max_rows=100)
    sink.process([message()])  # type: ignore[list-item]

    sink.close()

    assert len(appended) == 1
    assert consumer.commits == 1
    assert consumer.closed
