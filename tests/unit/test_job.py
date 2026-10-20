from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytest.importorskip("pyspark")

from pipeline.ingestion.dlq import ERROR_TYPE_HEADER  # noqa: E402
from pipeline.streaming.job import to_dlq_records, to_kafka_records  # noqa: E402


def test_kafka_records_are_keyed_by_symbol_with_json_value(spark: SparkSession) -> None:
    features = spark.createDataFrame(
        [("BTC-USD", datetime(2026, 10, 5, 12, 0), datetime(2026, 10, 5, 12, 1), 2, 107.5)],
        "symbol string, window_start timestamp, window_end timestamp, "
        "trade_count long, vwap double",
    )

    [record] = to_kafka_records(features).collect()

    assert record["key"] == "BTC-USD"
    assert json.loads(record["value"]) == {
        "symbol": "BTC-USD",
        # Session time zone is UTC, so timestamps are serialized with a Z offset.
        "window_start": "2026-10-05T12:00:00.000Z",
        "window_end": "2026-10-05T12:01:00.000Z",
        "trade_count": 2,
        "vwap": 107.5,
    }


def test_dlq_records_match_the_producer_envelope_with_a_replay_pointer(
    spark: SparkSession,
) -> None:
    dead_letters = spark.createDataFrame(
        [(b"BTC-USD", b"\x01\xff", "not_confluent_framed", "no header", 3, 42)],
        "key binary, value binary, error_type string, error_message string, "
        "kafka_partition int, kafka_offset long",
    )

    [record] = to_dlq_records(dead_letters, "market.trades.raw").collect()
    envelope = json.loads(record["value"])

    assert record["key"] == b"BTC-USD"
    assert [(h["key"], bytes(h["value"])) for h in record["headers"]] == [
        (ERROR_TYPE_HEADER, b"not_confluent_framed")
    ]
    assert base64.b64decode(envelope.pop("raw_payload")) == b"\x01\xff"
    assert isinstance(envelope.pop("failed_at"), int)
    assert envelope == {
        "payload_encoding": "base64",
        "error_type": "not_confluent_framed",
        "error_message": "no header",
        "source": "kafka://market.trades.raw/3/42",
    }
