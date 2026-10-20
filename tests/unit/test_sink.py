from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

pytest.importorskip("feast")

from pipeline.features.dead_letters import DeadLetterError  # noqa: E402
from pipeline.features.iceberg import TABLE_SCHEMA  # noqa: E402
from pipeline.features.sink import (  # noqa: E402
    ERROR_EXPECTATION_FAILED,
    ERROR_INVALID_MESSAGE,
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

    def key(self) -> bytes:
        return b"BTC-USD"

    def topic(self) -> str:
        return "market.features.1m"

    def partition(self) -> int:
        return 2

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


class FakeDeadLetters:
    def __init__(self, events: list[str] | None = None, fail_flush: bool = False) -> None:
        self.sent: list[tuple[Any, str, str]] = []
        self.events = events if events is not None else []
        self.fail_flush = fail_flush

    def send(self, message: Any, error_type: str, error_message: str) -> None:
        self.sent.append((message, error_type, error_message))

    def flush(self) -> None:
        self.events.append("dlq_flush")
        if self.fail_flush:
            raise DeadLetterError("broker down")


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


def message(offset: int = 0, **overrides: Any) -> FakeMessage:
    return FakeMessage(json.dumps({**FEATURE_JSON, **overrides}).encode(), offset=offset)


def no_failures(frame: pd.DataFrame) -> dict[int, list[str]]:
    return {}


def make_sink(
    append: Any = None,
    max_rows: int = 100,
    clock: FakeClock | None = None,
    dead_letters: FakeDeadLetters | None = None,
    validate: Callable[[pd.DataFrame], dict[int, list[str]]] = no_failures,
) -> tuple[FeatureSink, FakeConsumer, FakeStore, list[pa.Table]]:
    consumer, store = FakeConsumer(), FakeStore()
    appended: list[pa.Table] = []
    sink = FeatureSink(
        consumer=consumer,  # type: ignore[arg-type]
        store=store,  # type: ignore[arg-type]
        append=append or appended.append,
        buffer=AppendBuffer(max_rows=max_rows, max_age_seconds=60, clock=clock or FakeClock()),
        dead_letters=dead_letters or FakeDeadLetters(),
        validate=validate,
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


def test_invalid_messages_are_dead_lettered_and_counted() -> None:
    dead_letters = FakeDeadLetters()
    sink, _, store, _ = make_sink(dead_letters=dead_letters)
    bad_json, bad_vwap = FakeMessage(b"{not json"), message(vwap=-1.0)

    sink.process([bad_json, bad_vwap, message()])  # type: ignore[list-item]

    assert sink.stats.received == 3
    assert sink.stats.invalid == 2
    assert [(m, t) for m, t, _ in dead_letters.sent] == [
        (bad_json, ERROR_INVALID_MESSAGE),
        (bad_vwap, ERROR_INVALID_MESSAGE),
    ]
    [(_, frame)] = store.pushes
    assert len(frame) == 1


def test_rows_breaking_a_rule_reach_neither_store() -> None:
    dead_letters = FakeDeadLetters()
    sink, _, store, _ = make_sink(
        max_rows=1,
        dead_letters=dead_letters,
        validate=lambda frame: {1: ["vwap_within_price_range", "symbol_format"]},
    )
    good, bad = message(), message(offset=7, symbol="ETH-USD")

    sink.process([good, bad])  # type: ignore[list-item]

    [(_, frame)] = store.pushes
    assert list(frame["symbol"]) == ["BTC-USD"]
    assert list(frame.index) == [0]  # re-indexed after dropping the bad row
    assert sink.stats.rejected == 1
    assert sink.stats.pushed_online == sink.stats.appended_offline == 1
    assert dead_letters.sent == [
        (bad, ERROR_EXPECTATION_FAILED, "failed rules: vwap_within_price_range, symbol_format")
    ]


def test_batch_where_every_row_breaks_a_rule_stores_nothing() -> None:
    sink, _, store, _ = make_sink(validate=lambda frame: {0: ["symbol_format"]})

    sink.process([message()])  # type: ignore[list-item]

    assert store.pushes == []
    assert sink._buffer.records == []


def test_dead_letters_are_confirmed_before_offsets_are_committed() -> None:
    events: list[str] = []
    sink, consumer, _, _ = make_sink(max_rows=1, dead_letters=FakeDeadLetters(events))
    consumer.commit = lambda asynchronous=True: events.append("commit")  # type: ignore[method-assign]

    sink.process([message()])  # type: ignore[list-item]

    assert events == ["dlq_flush", "commit"]


def test_undelivered_dead_letters_stop_the_sink_without_committing() -> None:
    """Committing would lose the dead letter; crashing lets Kafka redeliver instead."""
    sink, consumer, _, _ = make_sink(max_rows=1, dead_letters=FakeDeadLetters(fail_flush=True))

    with pytest.raises(DeadLetterError):
        sink.process([message()])  # type: ignore[list-item]

    assert consumer.commits == 0


def test_real_suite_rejects_an_inconsistent_window() -> None:
    pytest.importorskip("great_expectations")
    from pipeline.governance.feature_checks import FeatureValidator

    dead_letters = FakeDeadLetters()
    sink, _, store, _ = make_sink(dead_letters=dead_letters, validate=FeatureValidator().failures)

    # Passes pydantic (all positive) but the VWAP is above the window's max price.
    sink.process([message(vwap=500.0), message()])  # type: ignore[list-item]

    [(_, error_type, error_message)] = dead_letters.sent
    assert error_type == ERROR_EXPECTATION_FAILED
    assert "vwap_within_price_range" in error_message
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


def test_feature_dlq_topic_is_not_taken_from_the_trade_dlq_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # All settings classes read the same .env, which sets KAFKA_TOPIC_DLQ for the producer.
    from pipeline.features.config import FeatureStoreSettings

    monkeypatch.setenv("KAFKA_TOPIC_DLQ", "market.trades.dlq")

    settings = FeatureStoreSettings(s3_access_key="k", s3_secret_key="s")  # type: ignore[arg-type]

    assert settings.kafka_topic_features_dlq == "market.features.dlq"
