import json
from collections.abc import Callable
from typing import Any

import pytest

from pipeline.ingestion.config import ProducerSettings
from pipeline.ingestion.dlq import ERROR_TYPE_HEADER
from pipeline.ingestion.producer import TradeRouter

NOW_MS = 1_790_714_413_660

GOOD_TRADE = {
    "product_id": "BTC-USD",
    "trade_id": "1",
    "price": "83592.48",
    "size": "0.5",
    "time": "2026-09-29T20:40:13.543036Z",
    "side": "SELL",
}
BAD_TRADE = {**GOOD_TRADE, "trade_id": "2", "price": "abc"}


def market_trades(*trades: dict[str, Any]) -> str:
    return json.dumps(
        {"channel": "market_trades", "events": [{"type": "update", "trades": list(trades)}]}
    )


class FakeProducer:
    def __init__(self, buffer_errors: int = 0) -> None:
        self.produced: list[dict[str, Any]] = []
        self.polls: list[float] = []
        self._buffer_errors = buffer_errors

    def produce(self, topic: str, **kwargs: Any) -> None:
        if self._buffer_errors:
            self._buffer_errors -= 1
            raise BufferError("queue full")
        self.produced.append({"topic": topic, **kwargs})

    def poll(self, timeout: float) -> int:
        self.polls.append(timeout)
        return 0

    def flush(self, timeout: float) -> int:
        return 0

    def topics(self) -> list[str]:
        return [p["topic"] for p in self.produced]


class FakeMessage:
    def topic(self) -> str:
        return "market.trades.raw"

    def key(self) -> bytes:
        return b"BTC-USD"


def fake_serializer(record: dict[str, Any]) -> bytes:
    return json.dumps(record).encode()


@pytest.fixture
def settings() -> ProducerSettings:
    return ProducerSettings(market_ws_url="wss://example/ws", _env_file=None)  # type: ignore[call-arg]


def make_router(
    settings: ProducerSettings,
    producer: FakeProducer,
    serialize: Callable[[dict[str, Any]], bytes] = fake_serializer,
) -> TradeRouter:
    return TradeRouter(producer, serialize, settings, clock_ms=lambda: NOW_MS)


def test_valid_trade_goes_to_raw_topic_keyed_by_symbol(settings: ProducerSettings) -> None:
    producer = FakeProducer()
    router = make_router(settings, producer)

    router.handle(market_trades(GOOD_TRADE))

    [sent] = producer.produced
    assert sent["topic"] == "market.trades.raw"
    assert sent["key"] == b"BTC-USD"
    assert json.loads(sent["value"])["ingest_time"] == NOW_MS
    assert sent["on_delivery"] == router.on_delivery
    assert producer.polls == [0]
    assert (router.stats.messages, router.stats.trades, router.stats.dead_lettered) == (1, 1, 0)


def test_mixed_message_splits_between_raw_and_dlq(settings: ProducerSettings) -> None:
    producer = FakeProducer()
    router = make_router(settings, producer)

    router.handle(market_trades(GOOD_TRADE, BAD_TRADE))

    assert sorted(producer.topics()) == ["market.trades.dlq", "market.trades.raw"]
    [dlq] = [p for p in producer.produced if p["topic"] == "market.trades.dlq"]
    assert "key" not in dlq
    assert dlq["headers"] == [(ERROR_TYPE_HEADER, b"validation_error")]
    envelope = json.loads(dlq["value"])
    assert json.loads(envelope["raw_payload"]) == BAD_TRADE
    assert envelope["source"] == "wss://example/ws"
    assert envelope["failed_at"] == NOW_MS
    assert (router.stats.trades, router.stats.dead_lettered) == (1, 1)


def test_malformed_message_goes_to_dlq_whole(settings: ProducerSettings) -> None:
    producer = FakeProducer()
    router = make_router(settings, producer)

    router.handle("{not json")

    [sent] = producer.produced
    assert sent["topic"] == "market.trades.dlq"
    assert json.loads(sent["value"])["raw_payload"] == "{not json"


def test_heartbeat_produces_nothing(settings: ProducerSettings) -> None:
    producer = FakeProducer()
    router = make_router(settings, producer)

    router.handle('{"channel":"heartbeats","events":[{"heartbeat_counter":1}]}')

    assert producer.produced == []
    assert router.stats.messages == 1


def test_source_error_is_counted_not_dead_lettered(settings: ProducerSettings) -> None:
    producer = FakeProducer()
    router = make_router(settings, producer)

    router.handle('{"type":"error","message":"failure to subscribe"}')

    assert producer.produced == []
    assert router.stats.source_errors == 1


def test_serialization_failure_goes_to_dlq(settings: ProducerSettings) -> None:
    def failing_serializer(record: dict[str, Any]) -> bytes:
        raise ValueError("not a valid long")

    producer = FakeProducer()
    router = make_router(settings, producer, failing_serializer)

    router.handle(market_trades(GOOD_TRADE))

    [sent] = producer.produced
    assert sent["topic"] == "market.trades.dlq"
    envelope = json.loads(sent["value"])
    assert envelope["error_type"] == "serialization_error"
    assert json.loads(envelope["raw_payload"])["symbol"] == "BTC-USD"


def test_full_queue_drains_then_retries_once(settings: ProducerSettings) -> None:
    producer = FakeProducer(buffer_errors=1)
    router = make_router(settings, producer)

    router.handle(market_trades(GOOD_TRADE))

    assert len(producer.produced) == 1
    assert producer.polls == [1.0, 0]


def test_queue_still_full_after_retry_raises(settings: ProducerSettings) -> None:
    router = make_router(settings, FakeProducer(buffer_errors=2))

    with pytest.raises(BufferError):
        router.handle(market_trades(GOOD_TRADE))


def test_delivery_callback_counts_success_and_failure(settings: ProducerSettings) -> None:
    router = make_router(settings, FakeProducer())

    router.on_delivery(None, FakeMessage())  # type: ignore[arg-type]
    router.on_delivery("broker unavailable", FakeMessage())  # type: ignore[arg-type]

    assert router.stats.delivered == 1
    assert router.stats.delivery_failed == 1


def test_product_ids_are_parsed_from_comma_separated_env() -> None:
    settings = ProducerSettings(market_product_ids=" btc-usd, ETH-USD ,", _env_file=None)  # type: ignore[call-arg]

    assert settings.product_ids == ["BTC-USD", "ETH-USD"]
