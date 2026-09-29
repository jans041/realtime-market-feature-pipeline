"""Parsing and validation of exchange trade messages.

Everything here is pure (no I/O), so it can be unit tested without a network,
a broker, or a Schema Registry.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

PositiveFinite = Annotated[float, Field(gt=0, allow_inf_nan=False)]


class TradeParseError(Exception):
    """Raised when a raw message cannot be turned into a valid Trade.

    ``error_type`` is a short, stable label (used as a DLQ header and for metrics);
    the exception message carries the human-readable detail.
    """

    def __init__(self, error_type: str, message: str) -> None:
        super().__init__(message)
        self.error_type = error_type


class BinanceTradeEvent(BaseModel):
    """Wire format of a Binance(.US) ``<symbol>@trade`` event.

    Binance sends numbers as strings (e.g. ``"p": "65000.10"``); Pydantic coerces them
    to float and rejects non-numeric, non-positive, NaN or infinite values.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    event_type: Literal["trade"] = Field(alias="e")
    symbol: str = Field(alias="s", min_length=1)
    trade_id: int = Field(alias="t", ge=0)
    price: PositiveFinite = Field(alias="p")
    quantity: PositiveFinite = Field(alias="q")
    trade_time_ms: int = Field(alias="T", gt=0)
    is_buyer_maker: bool = Field(alias="m")


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


def parse_trade(raw: str | bytes, *, exchange: str, ingest_time_ms: int) -> Trade:
    """Parse one WebSocket message into a Trade.

    Accepts both the raw-stream format (``/ws/btcusdt@trade``) and the combined-stream
    envelope (``/stream?streams=...``), which wraps the event as ``{"stream": ..., "data": {...}}``.

    Raises:
        TradeParseError: if the message is not JSON, not a trade event, or fails validation.
    """
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise TradeParseError("invalid_json", str(exc)) from exc

    if isinstance(payload, dict) and "stream" in payload and "data" in payload:
        payload = payload["data"]
    if not isinstance(payload, dict):
        raise TradeParseError(
            "invalid_shape", f"expected JSON object, got {type(payload).__name__}"
        )

    try:
        event = BinanceTradeEvent.model_validate(payload)
    except ValidationError as exc:
        raise TradeParseError("validation_error", _summarize(exc)) from exc

    return Trade(
        exchange=exchange,
        symbol=event.symbol.upper(),
        trade_id=event.trade_id,
        price=event.price,
        quantity=event.quantity,
        is_buyer_maker=event.is_buyer_maker,
        event_time=event.trade_time_ms,
        ingest_time=ingest_time_ms,
    )


def _summarize(exc: ValidationError) -> str:
    """Compact one-line summary, e.g. ``p: Input should be greater than 0``."""
    return "; ".join(
        f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}" for err in exc.errors()
    )
