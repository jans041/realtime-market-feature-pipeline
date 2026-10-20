"""Feature sink: market.features.1m -> Redis (online, immediately) + Iceberg (offline, batched).

Every batch is checked twice before it reaches either store: pydantic validates each
message's structure, then a Great Expectations suite checks the invariants that relate
fields to each other (see governance/feature_checks.py). Rejected messages go to
market.features.dlq with the reason.

Delivery is at-least-once: Kafka offsets are committed only after rows are durably
appended to Iceberg and dead letters are confirmed delivered. After a crash, uncommitted
messages are re-read, so a window can be written to Iceberg twice; Feast resolves
duplicates with ``ingested_at`` (latest wins), and the Redis online store ignores writes
older than the value it already holds.

Run:  python -m pipeline.features.sink      (or: docker compose up -d feature-sink)
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated, Any

import pandas as pd
import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, Message
from feast import FeatureStore
from feast.data_source import PushMode
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from pipeline.features.config import FeatureStoreSettings
from pipeline.features.dead_letters import (
    DeadLetters,
    KafkaDeadLetters,
    build_dead_letter_producer,
)
from pipeline.features.iceberg import TABLE_SCHEMA, ensure_table, open_catalog
from pipeline.features.store import get_store
from pipeline.governance.feature_checks import FeatureValidator

logger = logging.getLogger(__name__)

PUSH_SOURCE = "trade_features_1m_push"
ERROR_INVALID_MESSAGE = "invalid_feature_message"
ERROR_EXPECTATION_FAILED = "feature_expectation_failed"
ARROW_SCHEMA = TABLE_SCHEMA.as_arrow()

PositiveFinite = Annotated[float, Field(gt=0, allow_inf_nan=False)]
NonNegativeFinite = Annotated[float, Field(ge=0, allow_inf_nan=False)]


class FeatureRow(BaseModel):
    """One message from market.features.1m, validated before it reaches either store."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    symbol: str = Field(min_length=1)
    window_start: AwareDatetime
    window_end: AwareDatetime
    trade_count: int = Field(ge=1)
    volume: PositiveFinite
    vwap: PositiveFinite
    price_min: PositiveFinite
    price_max: PositiveFinite
    buy_volume: NonNegativeFinite
    sell_volume: NonNegativeFinite


def parse_feature_message(value: bytes | str) -> FeatureRow:
    """Raises ``ValueError`` (incl. pydantic's ``ValidationError``) for bad messages."""
    return FeatureRow.model_validate(json.loads(value))


def to_records(rows: Iterable[FeatureRow], ingested_at: datetime) -> list[dict[str, Any]]:
    return [{**row.model_dump(), "ingested_at": ingested_at} for row in rows]


def to_arrow(records: list[dict[str, Any]]) -> pa.Table:
    """Arrow table matching the Iceberg schema exactly (types and non-nullability)."""
    return pa.Table.from_pylist(records, schema=ARROW_SCHEMA)


def to_push_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    """DataFrame for ``FeatureStore.push``: entity, timestamps and feature columns."""
    frame = pd.DataFrame.from_records(records)
    for column in ("window_start", "window_end", "ingested_at"):
        frame[column] = pd.to_datetime(frame[column], utc=True)
    return frame


@dataclass
class AppendBuffer:
    """Collects rows for Iceberg; flush when ``max_rows`` or ``max_age_seconds`` is reached.

    Fewer, larger appends keep the table from filling up with tiny files and snapshots.
    """

    max_rows: int
    max_age_seconds: float
    clock: Callable[[], float] = time.monotonic
    records: list[dict[str, Any]] = field(default_factory=list)
    _first_added_at: float | None = None

    def add(self, records: list[dict[str, Any]]) -> None:
        if records and not self.records:
            self._first_added_at = self.clock()
        self.records.extend(records)

    def should_flush(self) -> bool:
        if not self.records or self._first_added_at is None:
            return False
        too_many = len(self.records) >= self.max_rows
        too_old = self.clock() - self._first_added_at >= self.max_age_seconds
        return too_many or too_old

    def drain(self) -> list[dict[str, Any]]:
        records, self.records, self._first_added_at = self.records, [], None
        return records


@dataclass
class SinkStats:
    received: int = 0
    invalid: int = 0  # failed parsing/pydantic
    rejected: int = 0  # parsed, but broke a data quality rule
    pushed_online: int = 0
    appended_offline: int = 0


class FeatureSink:
    def __init__(
        self,
        consumer: Consumer,
        store: FeatureStore,
        append: Callable[[pa.Table], None],
        buffer: AppendBuffer,
        dead_letters: DeadLetters,
        validate: Callable[[pd.DataFrame], dict[int, list[str]]],
    ) -> None:
        """``validate`` maps row positions in a batch to the rules each row broke."""
        self._consumer = consumer
        self._store = store
        self._append = append
        self._buffer = buffer
        self._dead_letters = dead_letters
        self._validate = validate
        self.stats = SinkStats()

    def poll(self, max_messages: int = 500, timeout: float = 1.0) -> None:
        """Consume up to ``max_messages``; returns after ``timeout`` even if none arrived,
        so time-based flushes still happen when the topic is quiet."""
        self.process(self._consumer.consume(num_messages=max_messages, timeout=timeout))

    def close(self) -> None:
        self.flush()
        self._consumer.close()

    def process(self, messages: list[Message]) -> None:
        parsed: list[tuple[Message, FeatureRow]] = []
        for message in messages:
            error = message.error()
            if error is not None:
                if error.code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", error)
                continue
            self.stats.received += 1
            try:
                parsed.append((message, parse_feature_message(message.value() or b"")))
            except (ValueError, ValidationError) as exc:
                self.stats.invalid += 1
                logger.warning(
                    "Dead-lettering invalid feature message at offset %s: %s",
                    message.offset(),
                    exc,
                )
                self._dead_letters.send(message, ERROR_INVALID_MESSAGE, str(exc))

        if parsed:
            self._store_valid(parsed)

        if self._buffer.should_flush():
            self.flush()

    def _store_valid(self, parsed: list[tuple[Message, FeatureRow]]) -> None:
        records = to_records([row for _, row in parsed], ingested_at=datetime.now(UTC))
        frame = to_push_frame(records)
        # Validate before either store sees the batch: bad features are never served.
        failures = self._validate(frame)
        if failures:
            self._reject(parsed, failures)
            keep = [i for i in range(len(records)) if i not in failures]
            records = [records[i] for i in keep]
            frame = frame.iloc[keep].reset_index(drop=True)
        if not records:
            return
        # Online first: serving should see new features within seconds.
        self._store.push(PUSH_SOURCE, frame, to=PushMode.ONLINE)
        self.stats.pushed_online += len(records)
        self._buffer.add(records)

    def _reject(
        self, parsed: list[tuple[Message, FeatureRow]], failures: dict[int, list[str]]
    ) -> None:
        for position, rules in sorted(failures.items()):
            self._dead_letters.send(
                parsed[position][0], ERROR_EXPECTATION_FAILED, "failed rules: " + ", ".join(rules)
            )
        self.stats.rejected += len(failures)
        broken = sorted({rule for rules in failures.values() for rule in rules})
        logger.warning(
            "Dead-lettered %d of %d feature rows; rules broken: %s",
            len(failures),
            len(parsed),
            ", ".join(broken),
        )

    def flush(self) -> None:
        records = self._buffer.drain()
        if not records:
            return
        try:
            self._append(to_arrow(records))
        except Exception:
            # Keep the rows and DON'T commit offsets: the batch is retried on the next
            # flush, and if the process dies, Kafka redelivers it.
            self._buffer.add(records)
            logger.exception("Iceberg append of %d rows failed; will retry", len(records))
            return
        # Raises DeadLetterError if any dead letter is undelivered. Deliberately not caught:
        # committing now would lose it, and a restart makes Kafka redeliver the batch.
        self._dead_letters.flush()
        self._consumer.commit(asynchronous=False)
        self.stats.appended_offline += len(records)
        newest = max(r["window_end"] for r in records)
        logger.info(
            "Appended %d rows to Iceberg; newest window_end %s (%.1f s ago); stats %s",
            len(records),
            newest.isoformat(),
            (datetime.now(UTC) - newest).total_seconds(),
            self.stats,
        )


def build_consumer(settings: FeatureStoreSettings) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            # Commit manually, only after the Iceberg append (at-least-once).
            "enable.auto.commit": False,
            # First run: start from the oldest retained features (backfill).
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([settings.kafka_topic_features])
    return consumer


def main() -> None:
    settings = FeatureStoreSettings()  # type: ignore[call-arg]  # S3 credentials come from env
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    table = ensure_table(open_catalog(settings), settings)
    sink = FeatureSink(
        consumer=build_consumer(settings),
        store=get_store(settings),
        append=table.append,
        buffer=AppendBuffer(settings.flush_max_rows, settings.flush_interval_seconds),
        dead_letters=KafkaDeadLetters(
            build_dead_letter_producer(settings.kafka_bootstrap_servers),
            settings.kafka_topic_features_dlq,
        ),
        validate=FeatureValidator().failures,
    )
    logger.info(
        "Feature sink running: %s -> Redis + Iceberg %s, rejects -> %s "
        "(flush every %.0f s or %d rows)",
        settings.kafka_topic_features,
        settings.table_identifier,
        settings.kafka_topic_features_dlq,
        settings.flush_interval_seconds,
        settings.flush_max_rows,
    )
    try:
        while True:
            sink.poll()
    except KeyboardInterrupt:
        logger.info("Shutting down; flushing buffered rows")
    finally:
        sink.close()
        logger.info("Final %s", sink.stats)


if __name__ == "__main__":
    main()
