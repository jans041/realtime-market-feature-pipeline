"""Windowed per-symbol trade features."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURE_COLUMNS = (
    "trade_count",
    "volume",
    "vwap",
    "price_min",
    "price_max",
    "buy_volume",
    "sell_volume",
)


def compute_features(trades: DataFrame, *, window: str, slide: str, watermark: str) -> DataFrame:
    """Aggregate trades into per-symbol sliding-window features.

    One output row per (symbol, window): ``symbol``, ``window_start``, ``window_end``
    and ``FEATURE_COLUMNS``. A trade belongs to ``window / slide`` overlapping windows.

    Features:
      trade_count  number of trades
      volume       total quantity traded (base currency, e.g. BTC)
      vwap         volume-weighted average price: sum(price * qty) / sum(qty).
                   Unlike avg(price), a 5 BTC trade counts five times as much as a 1 BTC one.
      price_min/max  price range within the window
      buy_volume   quantity where the BUYER was the aggressor (crossed the spread),
                   i.e. is_buyer_maker is False
      sell_volume  quantity where the SELLER was the aggressor (is_buyer_maker is True)
    """
    quantity = F.col("quantity")
    seller_was_aggressor = F.col("is_buyer_maker")

    return (
        # 1. Watermark: event_time is the event-time column; wait this long for late trades.
        #    Must come before the groupBy. (Ignored in batch mode, e.g. most unit tests.)
        trades.withWatermark("event_time", watermark)
        # 2. Group trades by symbol and by the time window(s) each trade falls into.
        .groupBy("symbol", F.window("event_time", window, slide))
        # 3. One calculation per feature, each named with .alias().
        .agg(
            F.count("*").alias("trade_count"),
            F.sum(quantity).alias("volume"),
            # volume is never 0: the producer rejects trades with quantity <= 0.
            (F.sum(F.col("price") * quantity) / F.sum(quantity)).alias("vwap"),
            F.min("price").alias("price_min"),
            F.max("price").alias("price_max"),
            F.sum(F.when(~seller_was_aggressor, quantity).otherwise(0.0)).alias("buy_volume"),
            F.sum(F.when(seller_was_aggressor, quantity).otherwise(0.0)).alias("sell_volume"),
        )
        # 4. F.window produces a struct column named "window"; flatten it into two columns.
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            *FEATURE_COLUMNS,
        )
    )
