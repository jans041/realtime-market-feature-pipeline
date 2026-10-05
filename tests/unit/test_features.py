from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytest.importorskip("pyspark")

from pipeline.streaming.features import FEATURE_COLUMNS, compute_features  # noqa: E402

TradeRow = tuple[str, str, float, float, bool]  # (symbol, 'HH:MM:SS', price, qty, is_buyer_maker)

TRADE_SCHEMA = (
    "symbol string, event_time timestamp, price double, quantity double, is_buyer_maker boolean"
)
# Tumbling windows (slide == window): each trade is in exactly one window, which keeps
# expected values simple. Sliding behaviour has its own test.
TUMBLING = {"window": "1 minute", "slide": "1 minute", "watermark": "10 seconds"}


def at(hms: str) -> datetime:
    return datetime.fromisoformat(f"2026-10-05T{hms}")


def trades(spark: SparkSession, *rows: TradeRow) -> DataFrame:
    """Build input shaped like decode_trades output (only the columns features use)."""
    return spark.createDataFrame(
        [(sym, at(t), price, qty, maker) for sym, t, price, qty, maker in rows], TRADE_SCHEMA
    )


# The two trades worked out by hand, both in the 12:00:00-12:01:00 window:
#   count 2, volume 1 + 3 = 4, VWAP (100*1 + 110*3) / 4 = 107.5 (plain average would be 105)
#   trade 1: is_buyer_maker False -> buyer aggressed -> buy volume 1
#   trade 2: is_buyer_maker True  -> seller aggressed -> sell volume 3
HAND_WORKED: tuple[TradeRow, ...] = (
    ("BTC-USD", "12:00:05", 100.0, 1.0, False),
    ("BTC-USD", "12:00:15", 110.0, 3.0, True),
)


def test_output_columns(spark: SparkSession) -> None:
    df = compute_features(trades(spark, *HAND_WORKED), **TUMBLING)

    assert df.columns == ["symbol", "window_start", "window_end", *FEATURE_COLUMNS]


def test_counts_trades_in_one_window(spark: SparkSession) -> None:
    [row] = compute_features(trades(spark, *HAND_WORKED), **TUMBLING).collect()

    assert row["symbol"] == "BTC-USD"
    assert row["window_start"] == at("12:00:00")
    assert row["window_end"] == at("12:01:00")
    assert row["trade_count"] == 2


def test_volume_and_vwap(spark: SparkSession) -> None:
    [row] = compute_features(trades(spark, *HAND_WORKED), **TUMBLING).collect()

    assert row["volume"] == pytest.approx(4.0)
    # 107.5, not 105: weighted by quantity, not a plain average of prices.
    assert row["vwap"] == pytest.approx(107.5)


def test_price_range(spark: SparkSession) -> None:
    [row] = compute_features(trades(spark, *HAND_WORKED), **TUMBLING).collect()

    assert (row["price_min"], row["price_max"]) == (100.0, 110.0)


def test_buy_and_sell_volume_follow_the_aggressor(spark: SparkSession) -> None:
    [row] = compute_features(trades(spark, *HAND_WORKED), **TUMBLING).collect()

    assert row["buy_volume"] == pytest.approx(1.0)
    assert row["sell_volume"] == pytest.approx(3.0)
    assert row["buy_volume"] + row["sell_volume"] == pytest.approx(row["volume"])


def test_symbols_are_aggregated_separately(spark: SparkSession) -> None:
    df = trades(
        spark,
        *HAND_WORKED,
        ("ETH-USD", "12:00:20", 2000.0, 5.0, False),
    )

    rows = {r["symbol"]: r for r in compute_features(df, **TUMBLING).collect()}

    assert rows["BTC-USD"]["trade_count"] == 2
    assert rows["ETH-USD"]["trade_count"] == 1
    assert rows["ETH-USD"]["vwap"] == pytest.approx(2000.0)


def test_window_start_is_inclusive_and_end_exclusive(spark: SparkSession) -> None:
    df = trades(
        spark,
        ("BTC-USD", "12:00:59.999", 100.0, 1.0, False),
        ("BTC-USD", "12:01:00", 101.0, 1.0, False),  # exactly on the boundary -> next window
    )

    rows = compute_features(df, **TUMBLING).orderBy("window_start").collect()

    assert [(r["window_start"], r["trade_count"]) for r in rows] == [
        (at("12:00:00"), 1),
        (at("12:01:00"), 1),
    ]


def test_sliding_windows_overlap(spark: SparkSession) -> None:
    df = trades(spark, ("BTC-USD", "12:00:05", 100.0, 1.0, False))

    rows = (
        compute_features(df, window="1 minute", slide="10 seconds", watermark="10 seconds")
        .orderBy("window_start")
        .collect()
    )

    # 60s window / 10s slide = 6 windows contain the trade: those starting at a multiple
    # of 10s with start <= 12:00:05 < start + 60s, i.e. 11:59:10 through 12:00:00.
    assert [r["window_start"] for r in rows] == [
        at("11:59:10"),
        at("11:59:20"),
        at("11:59:30"),
        at("11:59:40"),
        at("11:59:50"),
        at("12:00:00"),
    ]
    assert all(r["trade_count"] == 1 for r in rows)


def _write_batch(source_dir: Path, name: str, rows: list[TradeRow]) -> None:
    """Drop a JSON-lines file into a streaming source directory.

    Written elsewhere and then renamed, so Spark never sees a half-written file.
    """
    lines = [
        json.dumps(
            {
                "symbol": sym,
                "event_time": at(t).isoformat(),
                "price": price,
                "quantity": qty,
                "is_buyer_maker": maker,
            }
        )
        for sym, t, price, qty, maker in rows
    ]
    staged = source_dir.parent / f"{name}.json"
    staged.write_text("\n".join(lines) + "\n", encoding="utf-8")
    staged.rename(source_dir / f"{name}.json")


def test_late_trades_beyond_watermark_are_dropped(spark: SparkSession, tmp_path: Path) -> None:
    """Watermarks only act in streaming mode, so this runs a real streaming query.

    Batch 1 moves the max event time to 12:01:30, so watermark = 12:01:30 - 10s = 12:01:20.
    That is past the end of the 12:00 window, so the window is finalized and emitted.
    Batch 2 then brings a trade for 12:00:30, which is too late and must be dropped.
    """
    source = tmp_path / "source"
    source.mkdir()
    stream = spark.readStream.schema(TRADE_SCHEMA).json(str(source))
    query = (
        compute_features(stream, **TUMBLING)
        .writeStream.format("memory")
        .queryName("watermark_test")
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .start()
    )
    try:
        _write_batch(
            source,
            "batch1",
            [
                ("BTC-USD", "12:00:05", 100.0, 1.0, False),
                ("BTC-USD", "12:01:30", 100.0, 1.0, False),
            ],
        )
        query.processAllAvailable()
        _write_batch(source, "batch2", [("BTC-USD", "12:00:30", 100.0, 1.0, False)])
        query.processAllAvailable()
        emitted_so_far = spark.sql("SELECT window_start FROM watermark_test").collect()
        # Batch 3 advances the watermark to 12:02:50, finalizing the 12:01 window. If the
        # late trade had been kept, a second 12:00 row would be emitted here too.
        _write_batch(source, "batch3", [("BTC-USD", "12:03:00", 100.0, 1.0, False)])
        query.processAllAvailable()
        rows = spark.sql(
            "SELECT window_start, trade_count FROM watermark_test ORDER BY window_start"
        ).collect()
    finally:
        query.stop()

    # Before batch 3, the 12:01 window wasn't emitted yet: append mode waits until the
    # watermark (12:01:20) passes the window end (12:02:00). That wait is append mode's
    # latency cost.
    assert [r["window_start"] for r in emitted_so_far] == [at("12:00:00")]
    # Each window emitted exactly once; the 12:00 window counts 1 trade, not 2.
    assert [(r["window_start"], r["trade_count"]) for r in rows] == [
        (at("12:00:00"), 1),
        (at("12:01:00"), 1),
    ]
