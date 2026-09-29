"""Exchange-neutral trade record and parsing errors."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


class TradeParseError(Exception):
    """Raised when a raw message cannot be turned into valid Trades.

    ``error_type`` is a short, stable label (used as a DLQ header and for metrics);
    the exception message carries the human-readable detail.
    """

    def __init__(self, error_type: str, message: str) -> None:
        super().__init__(message)
        self.error_type = error_type


@dataclass(frozen=True, slots=True)
class Trade:
    """Normalized trade; field names and types match ``schemas/trade.avsc``."""

    exchange: str
    symbol: str
    trade_id: int
    price: float
    quantity: float
    is_buyer_maker: bool
    event_time: int  # epoch millis, UTC
    ingest_time: int  # epoch millis, UTC

    def to_avro_dict(self) -> dict[str, Any]:
        return asdict(self)
