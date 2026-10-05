"""Windowed per-symbol trade features."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_features(trades: DataFrame, *, window: str, slide: str, watermark: str) -> DataFrame:
    """Aggregate trades into per-symbol sliding-window features.

    One output row per (symbol, window): ``symbol``, ``window_start``, ``window_end``
    and the feature columns.
    """
    return (
        # 1. Watermark: event_time is the event-time column; wait this long for late trades.
        #    Must come before the groupBy. (Ignored in batch mode, e.g. unit tests.)
        trades.withWatermark("event_time", watermark)
        # 2. Group trades by symbol and by the time window(s) each trade falls into.
        .groupBy("symbol", F.window("event_time", window, slide))
        # 3. One calculation per feature, each named with .alias().
        .agg(F.count("*").alias("trade_count"))
        # 4. F.window produces a struct column named "window"; flatten it into two columns.
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "trade_count",
        )
    )
