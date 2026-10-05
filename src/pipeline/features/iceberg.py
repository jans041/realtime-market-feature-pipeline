"""Iceberg offline-store table for windowed trade features.

The table is the offline store's source of truth: every feature row ever computed,
queried by Feast for point-in-time training datasets.

Create it once (idempotent):  python -m pipeline.features.iceberg
"""

from __future__ import annotations

import logging
import os

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import DoubleType, LongType, NestedField, StringType, TimestamptzType

from pipeline.features.config import FeatureStoreSettings

logger = logging.getLogger(__name__)

# Feast's DuckDB offline store opens Iceberg REST catalogs under this name.
FEAST_CATALOG_NAME = "feast_iceberg"

# All fields required: every window always has every feature, so a null means a bug.
# Field ids are Iceberg's stable column identity; they must never be reused or renumbered.
TABLE_SCHEMA = Schema(
    NestedField(1, "symbol", StringType(), required=True),
    NestedField(2, "window_start", TimestamptzType(), required=True),
    # Feast event timestamp: features are only known once the window has closed.
    NestedField(3, "window_end", TimestamptzType(), required=True),
    NestedField(4, "trade_count", LongType(), required=True),
    NestedField(5, "volume", DoubleType(), required=True),
    NestedField(6, "vwap", DoubleType(), required=True),
    NestedField(7, "price_min", DoubleType(), required=True),
    NestedField(8, "price_max", DoubleType(), required=True),
    NestedField(9, "buy_volume", DoubleType(), required=True),
    NestedField(10, "sell_volume", DoubleType(), required=True),
    # When the sink wrote the row; Feast's tie-breaker if a window is written twice.
    NestedField(11, "ingested_at", TimestamptzType(), required=True),
)

# Partition by day of window_end so date-range queries only read the relevant files.
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="window_end_day")
)


def catalog_properties(settings: FeatureStoreSettings) -> dict[str, str]:
    """REST catalog + S3 client settings.

    The catalog server doesn't send S3 settings to clients, so each client sets its own.
    """
    return {
        "type": "rest",
        "uri": settings.iceberg_catalog_uri,
        "warehouse": settings.iceberg_warehouse,
        "s3.endpoint": settings.s3_endpoint,
        "s3.region": settings.aws_region,
        "s3.access-key-id": settings.s3_access_key,
        "s3.secret-access-key": settings.s3_secret_key.get_secret_value(),
    }


def open_catalog(settings: FeatureStoreSettings) -> Catalog:
    return load_catalog("rtfp", **catalog_properties(settings))


def export_feast_catalog_env(settings: FeatureStoreSettings) -> None:
    """Make S3 settings visible to Feast's Iceberg reader.

    Feast's DuckDB offline store opens a REST catalog with only ``uri`` and ``warehouse``
    and ignores other catalog properties. PyIceberg also reads catalog config from
    ``PYICEBERG_CATALOG__<NAME>__<KEY>`` environment variables, so the S3 settings are
    supplied that way.
    """
    prefix = f"PYICEBERG_CATALOG__{FEAST_CATALOG_NAME.upper()}__"
    for key, value in catalog_properties(settings).items():
        if key.startswith("s3."):
            env_key = key.upper().replace(".", "__").replace("-", "_")
            os.environ[prefix + env_key] = value


def ensure_table(catalog: Catalog, settings: FeatureStoreSettings) -> Table:
    """Create the namespace and table if missing; return the table either way."""
    catalog.create_namespace_if_not_exists(settings.iceberg_namespace)
    table = catalog.create_table_if_not_exists(
        settings.table_identifier,
        schema=TABLE_SCHEMA,
        partition_spec=PARTITION_SPEC,
    )
    logger.info(
        "Iceberg table %s ready at %s (%d snapshots)",
        settings.table_identifier,
        table.location(),
        len(table.metadata.snapshots),
    )
    return table


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    settings = FeatureStoreSettings()  # type: ignore[call-arg]  # S3 credentials come from env
    ensure_table(open_catalog(settings), settings)


if __name__ == "__main__":
    main()
