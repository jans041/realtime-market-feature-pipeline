"""Producer configuration, loaded from environment variables (and `.env` if present).

Settings are validated once at startup so a missing or malformed value fails fast
with a clear error instead of surfacing later as a confusing runtime failure.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, PositiveFloat
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[3]


class ProducerSettings(BaseSettings):
    """Runtime settings for the WebSocket -> Kafka trade producer.

    Each field maps to an upper-case env var of the same name,
    e.g. ``kafka_bootstrap_servers`` <- ``KAFKA_BOOTSTRAP_SERVERS``.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    kafka_topic_raw: str = "market.trades.raw"
    kafka_topic_dlq: str = "market.trades.dlq"

    market_ws_url: str = "wss://advanced-trade-ws.coinbase.com"
    # Comma-separated Coinbase product ids, e.g. "BTC-USD,ETH-USD".
    market_product_ids: str = "BTC-USD,ETH-USD"
    # Coinbase re-sends the last ~50 trades on every subscribe; see coinbase.parse_message.
    include_snapshots: bool = False
    exchange: str = "coinbase"
    trade_schema_path: Path = REPO_ROOT / "schemas" / "trade.avsc"

    reconnect_base_seconds: PositiveFloat = 1.0
    reconnect_cap_seconds: PositiveFloat = 60.0
    stats_interval_seconds: PositiveFloat = 30.0
    flush_timeout_seconds: PositiveFloat = 10.0

    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR)$")

    @property
    def product_ids(self) -> list[str]:
        return [p.strip().upper() for p in self.market_product_ids.split(",") if p.strip()]

    @property
    def trade_value_subject(self) -> str:
        """Schema Registry subject for the raw topic's value (TopicNameStrategy)."""
        return f"{self.kafka_topic_raw}-value"
