import json
from collections.abc import Callable
from typing import Any

import pytest

from pipeline.ingestion.config import ProducerSettings
from pipeline.ingestion.dlq import ERROR_TYPE_HEADER
from pipeline.ingestion.producer import TradeRouter

NOW_MS = 1_727_600_000_500

VALID = json.dumps(
    {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "s": "BTCUSDT",
            "t": 1,
            "p": "65000.1",
            "q": "0.5",
            "T": 1_727_600_000_000,
            "m": False,
        },
    }
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


class FakeMessage:
    def topic(self) -> str:
        return "market.trades.raw"

    def key(self) -> bytes:
        return b"BTCUSDT"


def fake_serializer(record: dict[str, Any]) -> bytes:
    return json.dumps(record).encode()


@pytest.fixture
def settings() -> ProducerSettings:
    return ProducerSettings(market_ws_url="wss://example/stream", _env_file=None)  # type: ignore[call-arg]


def make_router(
    settings: ProducerSettings,
    producer: FakeProducer,
    serialize: Callable[[dict[str, Any]], bytes] = fake_serializer,
) -> TradeRouter:
    return TradeRouter(producer, serialize, settings, clock_ms=lambda: NOW_MS)


def test_valid_trade_goes_to_raw_topic_keyed_by_symbol(settings: ProducerSettings) -> None:
    producer = FakeProducer()
    router = make_router(settings, producer)

    router.handle(VALID)

    [sent] = producer.produced
    assert sent["topic"] == "market.trades.raw"
    assert sent["key"] == b"BTCUSDT"
    assert json.loads(sent["value"])["ingest_time"] == NOW_MS
    assert sent["on_delivery"] == router.on_delivery
    assert producer.polls == [0]
    assert router.stats.received == 1
    assert router.stats.dead_lettered == 0


def test_invalid_payload_goes_to_dlq(settings: ProducerSettings) -> None:
    producer = FakeProducer()
    router = make_router(settings, producer)

    router.handle('{"e":"trade","p":"abc"}')

    [sent] = producer.produced
    assert sent["topic"] == "market.trades.dlq"
    assert "key" not in sent
    assert sent["headers"] == [(ERROR_TYPE_HEADER, b"validation_error")]
    envelope = json.loads(sent["value"])
    assert envelope["raw_payload"] == '{"e":"trade","p":"abc"}'
    assert envelope["source"] == "wss://example/stream"
    assert envelope["failed_at"] == NOW_MS
    assert router.stats.dead_lettered == 1


def test_serialization_failure_goes_to_dlq(settings: ProducerSettings) -> None:
    def failing_serializer(record: dict[str, Any]) -> bytes:
        raise ValueError("not a valid long")

    producer = FakeProducer()
    router = make_router(settings, producer, failing_serializer)

    router.handle(VALID)

    [sent] = producer.produced
    assert sent["topic"] == "market.trades.dlq"
    assert json.loads(sent["value"])["error_type"] == "serialization_error"


def test_full_queue_drains_then_retries_once(settings: ProducerSettings) -> None:
    producer = FakeProducer(buffer_errors=1)
    router = make_router(settings, producer)

    router.handle(VALID)

    assert len(producer.produced) == 1
    assert producer.polls == [1.0, 0]


def test_queue_still_full_after_retry_raises(settings: ProducerSettings) -> None:
    router = make_router(settings, FakeProducer(buffer_errors=2))

    with pytest.raises(BufferError):
        router.handle(VALID)


def test_delivery_callback_counts_success_and_failure(settings: ProducerSettings) -> None:
    router = make_router(settings, FakeProducer())

    router.on_delivery(None, FakeMessage())  # type: ignore[arg-type]
    router.on_delivery("broker unavailable", FakeMessage())  # type: ignore[arg-type]

    assert router.stats.delivered == 1
    assert router.stats.delivery_failed == 1
