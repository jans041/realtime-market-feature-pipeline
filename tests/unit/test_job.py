from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytest.importorskip("pyspark")

from pipeline.streaming.job import to_kafka_records  # noqa: E402


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
