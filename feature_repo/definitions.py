"""Feast definitions: what a feature row is keyed by, where it lives, how fresh it must be.

Apply with:  python -m pipeline.features.store apply
"""

import os
from datetime import timedelta

from feast import Entity, FeatureView, Field, PushSource, ValueType
from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import IcebergSource
from feast.types import Float64, Int64

symbol = Entity(
    name="symbol",
    join_keys=["symbol"],
    value_type=ValueType.STRING,
    description="Trading pair in the exchange's notation, e.g. BTC-USD.",
)

# Offline store: every feature row ever computed, in Iceberg (written by the feature sink).
trade_features_iceberg = IcebergSource(
    name="trade_features_1m_iceberg",
    endpoint=os.environ.get("ICEBERG_CATALOG_URI", "http://localhost:8181"),
    # Feast's REST client puts `warehouse` into the URL path (/v1/{warehouse}/...), which
    # suits catalogs like Polaris where it names a catalog. Our catalog serves tables at
    # /v1/... with no prefix and already knows its storage location, so this stays empty.
    warehouse="",
    namespace="market",
    table="trade_features_1m",
    catalog_type="rest",
    # Event time = when the features became KNOWN: the end of the window, not its start.
    # Using window_start would leak up to a window's worth of future data into training.
    timestamp_field="window_end",
    # Tie-breaker if the same window is written twice: the most recently ingested row wins.
    created_timestamp_column="ingested_at",
    credential_vending=False,
    description="Windowed trade features, 1 min windows sliding every 10 s.",
)

# Online path: the feature sink pushes fresh rows straight to Redis via this source.
trade_features_push = PushSource(
    name="trade_features_1m_push",
    batch_source=trade_features_iceberg,
)

trade_features_1m = FeatureView(
    name="trade_features_1m",
    entities=[symbol],
    # A value older than this counts as missing (null), in both online lookups and
    # point-in-time joins. New rows arrive every ~10 s, so 5 min of silence means the
    # pipeline is down and stale features shouldn't be served.
    ttl=timedelta(minutes=5),
    schema=[
        Field(name="trade_count", dtype=Int64),
        Field(name="volume", dtype=Float64),
        Field(name="vwap", dtype=Float64),
        Field(name="price_min", dtype=Float64),
        Field(name="price_max", dtype=Float64),
        Field(name="buy_volume", dtype=Float64),
        Field(name="sell_volume", dtype=Float64),
    ],
    source=trade_features_push,
    online=True,
    tags={"team": "market-data", "window": "1m", "slide": "10s"},
)
