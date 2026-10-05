from __future__ import annotations

import os

import pytest
from pydantic import SecretStr

from pipeline.features.config import FeatureStoreSettings
from pipeline.features.iceberg import (
    FEAST_CATALOG_NAME,
    PARTITION_SPEC,
    TABLE_SCHEMA,
    catalog_properties,
    export_feast_catalog_env,
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


def test_feast_catalog_env_exports_only_s3_settings(
    settings: FeatureStoreSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    prefix = f"PYICEBERG_CATALOG__{FEAST_CATALOG_NAME.upper()}__"
    # setenv first so monkeypatch restores the original environment after the test.
    for key in ("S3__ENDPOINT", "S3__ACCESS_KEY_ID", "S3__SECRET_ACCESS_KEY", "S3__REGION"):
        monkeypatch.setenv(prefix + key, "placeholder")

    export_feast_catalog_env(settings)

    assert os.environ[prefix + "S3__ENDPOINT"] == "http://storage:8333"
    assert os.environ[prefix + "S3__ACCESS_KEY_ID"] == "key"
    assert os.environ[prefix + "S3__SECRET_ACCESS_KEY"] == "hunter2-do-not-log"
    assert prefix + "URI" not in os.environ


def test_secret_is_hidden_in_repr(settings: FeatureStoreSettings) -> None:
    assert "hunter2-do-not-log" not in repr(settings)
    assert "hunter2-do-not-log" not in str(settings.model_dump())
