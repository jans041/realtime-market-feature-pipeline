from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from pipeline.features.config import FeatureStoreSettings
from pipeline.features.iceberg import (
    FEAST_CATALOG_NAME,
    PARTITION_SPEC,
    TABLE_SCHEMA,
    catalog_properties,
    register_feast_catalog_config,
)


@pytest.fixture
def settings() -> FeatureStoreSettings:
    return FeatureStoreSettings(
        s3_access_key="key",
        s3_secret_key=SecretStr("hunter2-do-not-log"),
        s3_endpoint="http://storage:8333",
        _env_file=None,  # type: ignore[call-arg]
    )


def test_table_columns_match_spark_feature_output() -> None:
    """Drift guard: the Iceberg table must hold exactly what the Spark job produces."""
    pytest.importorskip("pyspark")
    from pipeline.streaming.features import FEATURE_COLUMNS

    names = [field.name for field in TABLE_SCHEMA.fields]

    assert names == ["symbol", "window_start", "window_end", *FEATURE_COLUMNS, "ingested_at"]


def test_all_columns_are_required() -> None:
    assert all(field.required for field in TABLE_SCHEMA.fields)


def test_partitioned_by_day_of_window_end() -> None:
    [partition] = PARTITION_SPEC.fields

    assert TABLE_SCHEMA.find_field(partition.source_id).name == "window_end"
    assert str(partition.transform) == "day"


def test_catalog_properties_include_s3_client_settings(settings: FeatureStoreSettings) -> None:
    props = catalog_properties(settings)

    assert props["s3.endpoint"] == "http://storage:8333"
    assert props["s3.secret-access-key"] == "hunter2-do-not-log"


def test_feast_catalog_gets_s3_settings_via_public_load_catalog(
    settings: FeatureStoreSettings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Feast opens its catalog with only uri/warehouse; our S3 settings must be merged in.

    Without them Feast's S3 client silently falls back to real AWS. Opening the catalog by
    name through PyIceberg's public load_catalog (a local SQLite catalog, so no network)
    proves the merge happens, and fails if PyIceberg's internal config object changes.
    """
    from pyiceberg import catalog as pyiceberg_catalog

    catalogs = pyiceberg_catalog._ENV_CONFIG.config.setdefault("catalog", {})
    assert isinstance(catalogs, dict)
    monkeypatch.setitem(catalogs, FEAST_CATALOG_NAME, {})  # restored after the test

    register_feast_catalog_config(settings)
    catalog = pyiceberg_catalog.load_catalog(
        FEAST_CATALOG_NAME, type="sql", uri=f"sqlite:///{(tmp_path / 'c.db').as_posix()}"
    )

    assert catalog.properties["s3.endpoint"] == "http://storage:8333"
    assert catalog.properties["s3.access-key-id"] == "key"
    assert catalog.properties["s3.secret-access-key"] == "hunter2-do-not-log"


def test_secret_is_hidden_in_repr(settings: FeatureStoreSettings) -> None:
    assert "hunter2-do-not-log" not in repr(settings)
    assert "hunter2-do-not-log" not in str(settings.model_dump())
