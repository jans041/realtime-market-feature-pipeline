"""Read features back out of the feature store.

    python -m pipeline.features.query online BTC-USD ETH-USD
    python -m pipeline.features.query historical BTC-USD 2026-10-05T20:55:45Z

online:     latest values from Redis, what a live model would use at prediction time.
historical: point-in-time join against Iceberg, what a training set would contain.
            For each (symbol, timestamp) it returns the newest feature row whose
            window_end <= timestamp (and within the feature view's TTL), so no value
            from the future can leak into training data.
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd
from feast import FeatureStore

from pipeline.features.config import FeatureStoreSettings
from pipeline.features.store import get_store

FEATURES = [
    f"trade_features_1m:{name}"
    for name in (
        "trade_count",
        "volume",
        "vwap",
        "price_min",
        "price_max",
        "buy_volume",
        "sell_volume",
    )
]


def online_features(store: FeatureStore, symbols: list[str]) -> pd.DataFrame:
    return store.get_online_features(
        features=FEATURES, entity_rows=[{"symbol": s} for s in symbols]
    ).to_df()


def historical_features(store: FeatureStore, entity_df: pd.DataFrame) -> pd.DataFrame:
    """``entity_df`` has ``symbol`` and ``event_timestamp`` (UTC) columns: the moments
    you want to know the features *as of*, e.g. when each training label was observed."""
    return store.get_historical_features(entity_df=entity_df, features=FEATURES).to_df()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    online = sub.add_parser("online", help="latest features from Redis")
    online.add_argument("symbols", nargs="+")
    hist = sub.add_parser("historical", help="point-in-time features from Iceberg")
    hist.add_argument("symbol")
    hist.add_argument("timestamps", nargs="+", help="ISO-8601, e.g. 2026-10-05T20:55:45Z")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    store = get_store(FeatureStoreSettings())  # type: ignore[call-arg]
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)

    if args.mode == "online":
        print(online_features(store, args.symbols).to_string(index=False))
    else:
        entity_df = pd.DataFrame(
            {
                "symbol": args.symbol,
                "event_timestamp": pd.to_datetime(args.timestamps, utc=True),
            }
        )
        print(historical_features(store, entity_df).to_string(index=False))


if __name__ == "__main__":
    main()
