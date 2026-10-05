from __future__ import annotations

import io
import json
import struct
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import fastavro
import pytest

from pipeline.ingestion.config import REPO_ROOT

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytest.importorskip("pyspark")

from pipeline.streaming.decode import (  # noqa: E402
    HEADER_LENGTH,
    decode_trades,
    load_avro_schema,
)

SCHEMA_PATH = REPO_ROOT / "schemas" / "trade.avsc"
SCHEMA_ID = 2

TRADE: dict[str, Any] = {
    "exchange": "coinbase",
    "symbol": "BTC-USD",
    "trade_id": 1_100_154_155,
    "price": 83592.48,
    "quantity": 0.00077733,
    "is_buyer_maker": False,
    "event_time": 1_790_714_413_543,  # 2026-09-29T20:40:13.543Z
    "ingest_time": 1_790_714_413_660,
}


def confluent_frame(record: dict[str, Any], schema_id: int = SCHEMA_ID) -> bytes:
    """Encode exactly like confluent-kafka's AvroSerializer: magic byte + id + payload."""
    schema = fastavro.parse_schema(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, record)
    return b"\x00" + struct.pack(">I", schema_id) + buffer.getvalue()


def kafka_rows(spark: SparkSession, *values: bytes) -> DataFrame:
    """Mimic the Kafka source's columns that decode_trades uses."""
    return spark.createDataFrame(
        [(value, 3, offset) for offset, value in enumerate(values)],
        "value binary, partition int, offset long",
    )


def decode(spark: SparkSession, *values: bytes) -> list[dict[str, Any]]:
    df = decode_trades(kafka_rows(spark, *values), load_avro_schema(SCHEMA_PATH))
    return [row.asDict() for row in df.collect()]


def test_decodes_trade_fields_and_metadata(spark: SparkSession) -> None:
    [row] = decode(spark, confluent_frame(TRADE))

    assert row == {
        **TRADE,
        "event_time": datetime(2026, 9, 29, 20, 40, 13, 543000),
        "ingest_time": datetime(2026, 9, 29, 20, 40, 13, 660000),
        "schema_id": SCHEMA_ID,
        "kafka_partition": 3,
        "kafka_offset": 0,
    }


def test_timestamps_are_utc(spark: SparkSession) -> None:
    [row] = decode(spark, confluent_frame(TRADE))

    # Spark returns naive datetimes in the session time zone, which build_session pins to UTC.
    assert row["event_time"].replace(tzinfo=UTC).timestamp() * 1000 == TRADE["event_time"]


def test_schema_id_is_read_from_header(spark: SparkSession) -> None:
    [row] = decode(spark, confluent_frame(TRADE, schema_id=70_000))

    assert row["schema_id"] == 70_000


def test_bad_records_are_dropped_not_fatal(spark: SparkSession) -> None:
    good = confluent_frame(TRADE)
    rows = decode(
        spark,
        good,
        b"\x01" + good[1:],  # wrong magic byte
        good[:HEADER_LENGTH],  # header only, no payload
        good[:12],  # truncated payload
        b'{"symbol":"BTC-USD"}',  # plain JSON, not Confluent-framed
    )

    assert [r["kafka_offset"] for r in rows] == [0]
