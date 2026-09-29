"""WebSocket -> Kafka trade producer.

Reads trades from the Coinbase Advanced Trade WebSocket, validates them, and produces:
  - valid trades as Avro to ``KAFKA_TOPIC_RAW`` (keyed by symbol, so per-symbol order is kept)
  - anything that can't be parsed or serialized as JSON to ``KAFKA_TOPIC_DLQ``

Run:  python -m pipeline.ingestion.producer
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import websockets
from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from pipeline.ingestion.backoff import ExponentialBackoff
from pipeline.ingestion.coinbase import CoinbaseSourceError, parse_message, subscribe_messages
from pipeline.ingestion.config import ProducerSettings
from pipeline.ingestion.dlq import build_dlq_record
from pipeline.ingestion.models import Trade, TradeParseError

logger = logging.getLogger(__name__)

TradeSerializer = Callable[[dict[str, Any]], bytes]
DeliveryCallback = Callable[[KafkaError | None, Message], None]
Headers = list[tuple[str, str | bytes | None]]


class KafkaProducerLike(Protocol):
    """The subset of ``confluent_kafka.Producer`` we use; lets tests pass a fake."""

    def produce(
        self,
        topic: str,
        *,
        value: bytes | None = ...,
        key: bytes | None = ...,
        headers: Headers | None = ...,
        on_delivery: DeliveryCallback | None = ...,
    ) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


@dataclass
class ProducerStats:
    messages: int = 0  # WebSocket messages, including heartbeats
    trades: int = 0  # valid trades handed to Kafka
    delivered: int = 0  # broker-acknowledged, raw + DLQ
    delivery_failed: int = 0
    dead_lettered: int = 0
    source_errors: int = 0


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class TradeRouter:
    """Turns raw WebSocket messages into Kafka produce calls (raw topic or DLQ).

    Kept free of asyncio and network code so it can be unit tested with a fake producer.
    """

    def __init__(
        self,
        producer: KafkaProducerLike,
        serialize: TradeSerializer,
        settings: ProducerSettings,
        clock_ms: Callable[[], int] = now_ms,
    ) -> None:
        self._producer = producer
        self._serialize = serialize
        self._settings = settings
        self._clock_ms = clock_ms
        self.stats = ProducerStats()

    def handle(self, raw: str | bytes) -> None:
        self.stats.messages += 1
        received_at = self._clock_ms()

        try:
            parsed = parse_message(
                raw,
                exchange=self._settings.exchange,
                ingest_time_ms=received_at,
                include_snapshots=self._settings.include_snapshots,
            )
        except TradeParseError as exc:
            self._dead_letter(raw, exc.error_type, str(exc), received_at)
            return
        except CoinbaseSourceError as exc:
            self.stats.source_errors += 1
            logger.error("Coinbase reported an error: %s", exc)
            return

        for rejected in parsed.rejected:
            self._dead_letter(
                rejected.raw, rejected.error_type, rejected.error_message, received_at
            )
        for trade in parsed.trades:
            self._produce_trade(trade, received_at)

    def _produce_trade(self, trade: Trade, received_at: int) -> None:
        self.stats.trades += 1
        record = trade.to_avro_dict()
        try:
            value = self._serialize(record)
        except (ValueError, TypeError) as exc:
            # fastavro raises these when a record doesn't fit the schema.
            self._dead_letter(json.dumps(record), "serialization_error", str(exc), received_at)
            return

        self._produce(
            self._settings.kafka_topic_raw,
            key=trade.symbol.encode("utf-8"),
            value=value,
        )

    def on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        """Called from ``producer.poll()`` once the broker acks (or finally rejects) a message."""
        if err is not None:
            self.stats.delivery_failed += 1
            logger.error("Delivery failed topic=%s key=%s: %s", msg.topic(), msg.key(), err)
        else:
            self.stats.delivered += 1

    def _dead_letter(self, raw: str | bytes, error_type: str, message: str, at_ms: int) -> None:
        self.stats.dead_lettered += 1
        logger.warning("Dead-lettering message (%s): %s", error_type, message)
        record = build_dlq_record(
            raw,
            error_type=error_type,
            error_message=message,
            source=self._settings.market_ws_url,
            failed_at_ms=at_ms,
        )
        self._produce(self._settings.kafka_topic_dlq, value=record.value, headers=record.headers)

    def _produce(self, topic: str, **kwargs: Any) -> None:
        try:
            self._producer.produce(topic, on_delivery=self.on_delivery, **kwargs)
        except BufferError:
            # Local send queue is full (broker slow or down): drain callbacks, then retry once.
            logger.warning("Producer queue full; waiting for deliveries before retrying")
            self._producer.poll(1.0)
            self._producer.produce(topic, on_delivery=self.on_delivery, **kwargs)
        # Serve delivery callbacks without blocking the event loop.
        self._producer.poll(0)


def build_producer(settings: ProducerSettings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "market-trade-producer",
            # Durability: wait for all in-sync replicas; idempotence prevents duplicates
            # and reordering when the client retries.
            "acks": "all",
            "enable.idempotence": True,
            # Throughput: batch for up to 20 ms and compress batches.
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )


def build_trade_serializer(settings: ProducerSettings) -> TradeSerializer:
    """Build an Avro serializer bound to an already-registered schema.

    Fails fast at startup if the schema isn't registered, rather than dead-lettering
    every trade later. Register it with ``python -m pipeline.ingestion.register_schema``.
    """
    schema_str = settings.trade_schema_path.read_text(encoding="utf-8")
    client = SchemaRegistryClient({"url": settings.schema_registry_url})
    subject = settings.trade_value_subject

    try:
        registered = client.lookup_schema(subject, Schema(schema_str, "AVRO"))
    except SchemaRegistryError as exc:
        raise SystemExit(
            f"Schema {settings.trade_schema_path.name} is not registered under subject "
            f"'{subject}' ({exc}). Run: python -m pipeline.ingestion.register_schema"
        ) from exc
    logger.info(
        "Using schema id=%s version=%s for %s", registered.schema_id, registered.version, subject
    )

    avro = AvroSerializer(client, schema_str, conf={"auto.register.schemas": False})
    ctx = SerializationContext(settings.kafka_topic_raw, MessageField.VALUE)

    def serialize(record: dict[str, Any]) -> bytes:
        payload = avro(record, ctx)
        assert payload is not None  # only None when the input is None
        return payload

    return serialize


async def stream_trades(settings: ProducerSettings, router: TradeRouter) -> None:
    """Consume the WebSocket forever, reconnecting with jittered exponential backoff."""
    backoff = ExponentialBackoff(settings.reconnect_base_seconds, settings.reconnect_cap_seconds)

    while True:
        try:
            async with websockets.connect(settings.market_ws_url, open_timeout=10) as ws:
                logger.info("Connected to %s", settings.market_ws_url)
                # Subscriptions are per connection, so they're re-sent after every reconnect.
                for subscribe in subscribe_messages(settings.product_ids):
                    await ws.send(subscribe)
                logger.info("Subscribed to market_trades for %s", ", ".join(settings.product_ids))
                async for message in ws:
                    router.handle(message)
                    if backoff.attempt:
                        backoff.reset()
            logger.warning("WebSocket closed by server")
        except (OSError, websockets.WebSocketException) as exc:
            # OSError covers DNS failures, refused connections and timeouts.
            logger.warning("WebSocket error: %s: %s", type(exc).__name__, exc)

        delay = backoff.next_delay()
        logger.info("Reconnecting in %.1fs (attempt %d)", delay, backoff.attempt)
        await asyncio.sleep(delay)


async def log_stats(router: TradeRouter, interval_seconds: float) -> None:
    previous_delivered = router.stats.delivered
    while True:
        await asyncio.sleep(interval_seconds)
        stats = router.stats
        rate = (stats.delivered - previous_delivered) / interval_seconds
        previous_delivered = stats.delivered
        logger.info(
            "stats messages=%d trades=%d delivered=%d failed=%d dlq=%d rate=%.1f delivered/s",
            stats.messages,
            stats.trades,
            stats.delivered,
            stats.delivery_failed,
            stats.dead_lettered,
            rate,
        )


async def run(settings: ProducerSettings, router: TradeRouter) -> None:
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(stream_trades(settings, router))
        tasks.create_task(log_stats(router, settings.stats_interval_seconds))


def main() -> None:
    settings = ProducerSettings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)  # Schema Registry client request logs

    producer = build_producer(settings)
    router = TradeRouter(producer, build_trade_serializer(settings), settings)

    try:
        asyncio.run(run(settings, router))
    except KeyboardInterrupt:
        # Ctrl+C. (asyncio signal handlers aren't available on Windows, so we catch it here.)
        logger.info("Shutting down")
    finally:
        remaining = producer.flush(settings.flush_timeout_seconds)
        if remaining:
            logger.error("%d message(s) not delivered before shutdown", remaining)
        logger.info("Final %s", router.stats)


if __name__ == "__main__":
    main()
