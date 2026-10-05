from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytest.importorskip("pyspark")

from pipeline.streaming.features import compute_features  # noqa: E402

TUMBLING = {"window": "1 minute", "slide": "1 minute", "watermark": "30 seconds"}


def trades(spark: SparkSession, *rows: tuple[str, str, float, float, bool]) -> DataFrame:
    """Build input like decode_trades produces: (symbol, 'HH:MM:SS', price, qty, is_buyer_maker)."""
    return spark.createDataFrame(
        [(sym, datetime.fromisoformat(f"2026-10-05T{t}"), p, q, m) for sym, t, p, q, m in rows],
        "symbol string, event_time timestamp, "
        "price double, quantity double, is_buyer_maker boolean",
    )


def test_counts_trades_in_one_window(spark: SparkSession) -> None:
    # Arrange: two trades inside the same minute
    df = trades(
        spark,
        ("BTC-USD", "12:00:05", 100.0, 1.0, False),
        ("BTC-USD", "12:00:15", 110.0, 3.0, True),
    )

    # Act
    [row] = compute_features(df, **TUMBLING).collect()

    # Assert
    assert row["symbol"] == "BTC-USD"
    assert row["window_start"] == datetime(2026, 10, 5, 12, 0, 0)
    assert row["trade_count"] == 2
