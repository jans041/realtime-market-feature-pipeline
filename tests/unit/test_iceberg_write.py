"""Writes to a REAL Iceberg table (local SQLite catalog + local files), no services needed.

The sink's unit tests fake the append; this test exercises the actual write path:
our schema, our day partitioning, and the Arrow conversion together. It exists because
a partitioned append needs ``pyiceberg-core``, which faked tests could never catch.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import SecretStr

pytest.importorskip("feast")
pytest.importorskip("sqlalchemy")
if sys.platform == "win32":
    # PyIceberg's local FileIO turns file:///C:/... into the invalid path /C:/... on Windows.
    pytest.skip(
        "local Iceberg FileIO is broken on Windows; run in Docker: "
        "docker compose run --rm spark pytest",
        allow_module_level=True,
    )

from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

from pipeline.features.config import FeatureStoreSettings  # noqa: E402
from pipeline.features.iceberg import ensure_table  # noqa: E402
from pipeline.features.sink import parse_feature_message, to_arrow, to_records  # noqa: E402

INGESTED_AT = datetime(2026, 10, 5, 12, 1, 15, tzinfo=UTC)


def feature(symbol: str, window_end: str) -> str:
    return json.dumps(
        {
            "symbol": symbol,
            "window_start": "2026-10-05T12:00:00.000Z",
            "window_end": window_end,
            "trade_count": 2,
            "volume": 4.0,
            "vwap": 107.5,
            "price_min": 100.0,
            "price_max": 110.0,
            "buy_volume": 1.0,
            "sell_volume": 3.0,
        }
    )


@pytest.fixture
def catalog(tmp_path: Path) -> SqlCatalog:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        warehouse=warehouse.as_uri(),
    )


@pytest.fixture
def settings() -> FeatureStoreSettings:
    return FeatureStoreSettings(
        s3_access_key="unused",
        s3_secret_key=SecretStr("unused"),
        _env_file=None,  # type: ignore[call-arg]
    )


def test_sink_rows_append_to_partitioned_table_and_read_back(
    catalog: SqlCatalog, settings: FeatureStoreSettings
) -> None:
    table = ensure_table(catalog, settings)
    rows = [
        parse_feature_message(feature("BTC-USD", "2026-10-05T12:01:00.000Z")),
        parse_feature_message(feature("ETH-USD", "2026-10-06T00:00:10.000Z")),  # next day
    ]

    table.append(to_arrow(to_records(rows, INGESTED_AT)))

    read = table.scan().to_arrow().sort_by("symbol")
    assert read.column("symbol").to_pylist() == ["BTC-USD", "ETH-USD"]
    assert read.column("vwap").to_pylist() == [107.5, 107.5]
    assert read.column("ingested_at")[0].as_py() == INGESTED_AT
    # One file per day partition: the two rows fall on different days of window_end.
    partitions = table.inspect.partitions().column("partition").to_pylist()
    assert len(partitions) == 2


def test_ensure_table_is_idempotent(catalog: SqlCatalog, settings: FeatureStoreSettings) -> None:
    first = ensure_table(catalog, settings)
    second = ensure_table(catalog, settings)

    assert first.metadata.table_uuid == second.metadata.table_uuid
