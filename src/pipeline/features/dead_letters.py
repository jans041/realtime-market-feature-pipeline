"""Dead-letter output for the feature sink: rejected messages -> market.features.dlq.

Records use the same JSON envelope as the producer's trade DLQ (see ingestion/dlq.py), with
``source`` pointing at the exact message (``kafka://topic/partition/offset``) for replay.

Delivery must be confirmed before the sink commits Kafka offsets. Otherwise the sink could
commit past a rejected message that never reached the DLQ, and it would be lost silently.
So ``flush`` raises if anything is undelivered, and the sink lets that stop the process:
on restart, Kafka redelivers everything after the last committed offset.
"""

from __future__ import annotations

import logging
import time
from typing import Protocol

from confluent_kafka import KafkaError, Message, Producer

from pipeline.ingestion.dlq import build_dlq_record

logger = logging.getLogger(__name__)


class DeadLetterError(RuntimeError):
    """Some dead letters weren't confirmed delivered; offsets must not be committed."""


class DeadLetters(Protocol):
    def send(self, message: Message, error_type: str, error_message: str) -> None: ...

    def flush(self) -> None: ...


def message_source(message: Message) -> str:
    return f"kafka://{message.topic()}/{message.partition()}/{message.offset()}"


class KafkaDeadLetters:
    def __init__(self, producer: Producer, topic: str, flush_timeout: float = 10.0) -> None:
        self._producer = producer
        self._topic = topic
        self._flush_timeout = flush_timeout
        self._failed = 0

    def send(self, message: Message, error_type: str, error_message: str) -> None:
        record = build_dlq_record(
            message.value() or b"",
            error_type=error_type,
            error_message=error_message,
            source=message_source(message),
            failed_at_ms=int(time.time() * 1000),
        )
        # list is invariant, so widen to the element type produce() declares.
        headers: list[tuple[str, str | bytes | None]] = list(record.headers)
        self._producer.produce(
            self._topic,
            key=message.key(),  # same key as the original, so it lands in a matching partition
            value=record.value,
            headers=headers,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)  # serve delivery callbacks without blocking

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self._failed += 1
            logger.error("Dead letter not delivered to %s: %s", msg.topic(), err)

    def flush(self) -> None:
        remaining = self._producer.flush(self._flush_timeout)
        failed, self._failed = self._failed, 0
        if remaining or failed:
            raise DeadLetterError(
                f"{failed} dead letters failed and {remaining} are still queued for "
                f"{self._topic}; not committing offsets"
            )


def build_dead_letter_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "feature-sink-dlq",
            # Same durability settings as the trade producer.
            "acks": "all",
            "enable.idempotence": True,
        }
    )
