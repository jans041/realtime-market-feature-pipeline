"""Streaming job configuration, loaded from environment variables (and `.env` if present)."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from pipeline.ingestion.config import REPO_ROOT


class StreamingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw: str = "market.trades.raw"
    kafka_topic_features: str = "market.features.1m"
    trade_schema_path: Path = REPO_ROOT / "schemas" / "trade.avsc"

    feature_window: str = "1 minute"
    feature_slide: str = "10 seconds"
    # Measured exchange->producer lag is p99 ~234 ms; 10 s leaves ample headroom for
    # out-of-order trades. In append mode each window is emitted ~this long after it closes.
    feature_watermark: str = "10 seconds"
    trigger_interval: str = "10 seconds"

    # Only used on the very first start; afterwards the checkpoint decides where to resume.
    starting_offsets: str = Field(default="earliest", pattern="^(earliest|latest)$")
    # Caps each micro-batch so catching up on a backlog doesn't create one giant batch.
    max_offsets_per_trigger: PositiveInt = 10_000
    checkpoint_dir: str = "/checkpoints/features-1m"

    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR)$")
