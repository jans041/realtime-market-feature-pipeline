from __future__ import annotations

import json
from typing import Any

import pytest

from pipeline.features.dead_letters import DeadLetterError, KafkaDeadLetters
from pipeline.ingestion.dlq import ERROR_TYPE_HEADER


class FakeMessage:
    def value(self) -> bytes:
        return b'{"symbol":"BTC-USD","vwap":500.0}'

    def key(self) -> bytes:
        return b"BTC-USD"

    def topic(self) -> str:
        return "market.features.1m"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 41


class FakeProducer:
    def __init__(self, remaining: int = 0, deliver_error: Any = None) -> None:
        self.produced: list[dict[str, Any]] = []
        self.remaining = remaining
        self.deliver_error = deliver_error

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.produced.append({"topic": topic, **kwargs})

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        # Simulate the client running delivery callbacks during flush().
        for record in self.produced:
            record["on_delivery"](self.deliver_error, FakeMessage())
        return self.remaining


def make(producer: FakeProducer) -> KafkaDeadLetters:
    dead_letters = KafkaDeadLetters(producer, "market.features.dlq")  # type: ignore[arg-type]
    dead_letters.send(FakeMessage(), "feature_expectation_failed", "failed rules: x")  # type: ignore[arg-type]
    return dead_letters


def test_dead_letter_uses_the_shared_envelope_and_points_at_the_source() -> None:
    producer = FakeProducer()
    make(producer)

    [record] = producer.produced
    envelope = json.loads(record["value"])
    assert record["topic"] == "market.features.dlq"
    assert record["key"] == b"BTC-USD"
    assert record["headers"] == [(ERROR_TYPE_HEADER, b"feature_expectation_failed")]
    assert envelope["raw_payload"] == '{"symbol":"BTC-USD","vwap":500.0}'
    assert envelope["source"] == "kafka://market.features.1m/2/41"
    assert envelope["error_message"] == "failed rules: x"


def test_flush_passes_when_everything_is_delivered() -> None:
    make(FakeProducer()).flush()


@pytest.mark.parametrize(
    "producer",
    [FakeProducer(remaining=1), FakeProducer(deliver_error="broker down")],
    ids=["still_queued", "delivery_failed"],
)
def test_flush_raises_when_anything_is_undelivered(producer: FakeProducer) -> None:
    with pytest.raises(DeadLetterError):
        make(producer).flush()


def test_failure_count_resets_after_it_is_reported() -> None:
    producer = FakeProducer(deliver_error="broker down")
    dead_letters = make(producer)
    with pytest.raises(DeadLetterError):
        dead_letters.flush()

    producer.produced, producer.deliver_error = [], None

    dead_letters.flush()
