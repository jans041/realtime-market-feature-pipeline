from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

pytest.importorskip("feast")

from feast import FeatureStore, FeatureView, FileSource  # noqa: E402
from feast.repo_config import RepoConfig  # noqa: E402

from pipeline.features.query import FEATURES, historical_features  # noqa: E402

DEFINITIONS = Path(__file__).parents[2] / "feature_repo" / "definitions.py"


def _load_definitions() -> ModuleType:
    spec = importlib.util.spec_from_file_location("feast_definitions", DEFINITIONS)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _row(window_end: str, ingested_at: str, trade_count: int) -> dict[str, object]:
    end = datetime.fromisoformat(window_end)
    return {
        "symbol": "BTC-USD",
        "window_start": end - timedelta(minutes=1),
        "window_end": end,
        "trade_count": trade_count,
        "volume": 1.0,
        "vwap": 100.0,
        "price_min": 99.0,
        "price_max": 101.0,
        "buy_volume": 0.5,
        "sell_volume": 0.5,
        "ingested_at": datetime.fromisoformat(ingested_at),
    }


@pytest.fixture
def store(tmp_path: Path) -> FeatureStore:
    """A real Feast store using the production feature view (same name, schema and TTL)
    and the same DuckDB offline engine, with only the source swapped for a local parquet
    file. Each window reaches the store ~30 s after it ends, as in the live pipeline."""
    rows = [
        _row("2026-10-05T12:01:00Z", "2026-10-05T12:01:31Z", trade_count=1),
        _row("2026-10-05T12:01:10Z", "2026-10-05T12:01:41Z", trade_count=2),
    ]
    parquet = tmp_path / "features.parquet"
    pd.DataFrame(rows).to_parquet(parquet)

    defs = _load_definitions()
    prod_view = defs.trade_features_1m
    view = FeatureView(
        name=prod_view.name,
        entities=[defs.symbol],
        ttl=prod_view.ttl,
        schema=prod_view.schema,
        source=FileSource(
            path=str(parquet),
            timestamp_field="window_end",
            created_timestamp_column="ingested_at",
        ),
    )
    config = RepoConfig(
        project="test_query",
        provider="local",
        registry=str(tmp_path / "registry.db"),
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
        offline_store={"type": "duckdb"},
        entity_key_serialization_version=3,
        repo_path=str(tmp_path),
    )
    feature_store = FeatureStore(config=config)
    feature_store.apply([defs.symbol, view])
    return feature_store


def _entities(*timestamps: str) -> pd.DataFrame:
    return pd.DataFrame(
        {"symbol": "BTC-USD", "event_timestamp": pd.to_datetime(list(timestamps), utc=True)}
    )


def test_plain_feast_join_leaks_rows_not_yet_written(store: FeatureStore) -> None:
    """Shows the leak is real: filtering on window_end alone, a query at 12:01:35 sees
    the 12:01:10 window, which wasn't written until 12:01:41."""
    df = store.get_historical_features(entity_df=_entities("2026-10-05T12:01:35Z"), features=FEATURES)
    assert df.to_df()["trade_count"].tolist() == [2]


def test_historical_features_only_returns_rows_already_written(store: FeatureStore) -> None:
    df = historical_features(
        store, _entities("2026-10-05T12:01:35Z", "2026-10-05T12:01:45Z", "2026-10-05T12:01:20Z")
    ).sort_values("event_timestamp")

    # 12:01:20 - nothing written yet (first row lands at 12:01:31).
    # 12:01:35 - only the 12:01:00 window is in the store.
    # 12:01:45 - the 12:01:10 window has landed and is newest.
    assert df["trade_count"].fillna(-1).astype(int).tolist() == [-1, 1, 2]
