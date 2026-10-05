"""Feature store configuration, loaded from environment variables (and `.env` if present)."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, PositiveFloat, PositiveInt, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from pipeline.ingestion.config import REPO_ROOT


class FeatureStoreSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_features: str = "market.features.1m"
    kafka_consumer_group: str = "feature-sink"

    iceberg_catalog_uri: str = "http://localhost:8181"
    iceberg_warehouse: str = "s3://warehouse/"
    iceberg_namespace: str = "market"
    iceberg_table: str = "trade_features_1m"

    s3_endpoint: str = "http://localhost:8333"
    s3_access_key: str
    s3_secret_key: SecretStr  # SecretStr keeps the value out of logs and reprs
    aws_region: str = "us-east-1"

    redis_connection_string: str = "localhost:6379"
    feature_repo_path: Path = REPO_ROOT / "feature_repo"

    # Iceberg append batching: every append writes files and a snapshot, so batch rows
    # to avoid the "small files problem". Flush on whichever limit is hit first.
    flush_interval_seconds: PositiveFloat = 60.0
    flush_max_rows: PositiveInt = 1_000

    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR)$")

    @property
    def table_identifier(self) -> str:
        return f"{self.iceberg_namespace}.{self.iceberg_table}"
