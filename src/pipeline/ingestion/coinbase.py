"""Coinbase Advanced Trade WebSocket: subscribe messages and ``market_trades`` parsing.

Everything here is pure (no I/O), so it can be unit tested without a network.
Docs: https://docs.cdp.coinbase.com/advanced-trade/docs/ws-channels

A ``market_trades`` message can carry several trades:

    {"channel": "market_trades", "sequence_num": 11, "events": [
        {"type": "update", "trades": [
            {"product_id": "BTC-USD", "trade_id": "1100154155", "price": "83592.48",
             "size": "0.00077733", "time": "2026-09-29T20:40:13.543036Z", "side": "SELL"}]}]}
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from pipeline.ingestion.models import Trade, TradeParseError

PositiveFinite = Annotated[float, Field(gt=0, allow_inf_nan=False)]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MS = timedelta(milliseconds=1)


class CoinbaseSourceError(Exception):
    """Coinbase sent ``{"type": "error", ...}``, e.g. a rejected subscription.

    This is an operational problem, not bad market data, so it is logged, not dead-lettered.
    """


class CoinbaseTrade(BaseModel):
    """One element of ``events[].trades[]``. Numbers arrive as strings and are coerced."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    product_id: str = Field(min_length=1)
    trade_id: int = Field(ge=0)
    price: PositiveFinite
    size: PositiveFinite
    time: AwareDatetime
    # Side of the MAKER order. Verified empirically against best bid/ask: SELL trades
    # print at the ask (a buyer crossed the spread), BUY trades print at the bid.
    side: Literal["BUY", "SELL"]


@dataclass(frozen=True, slots=True)
class RejectedTrade:
    raw: str
    error_type: str
    error_message: str


@dataclass(frozen=True, slots=True)
class ParsedMessage:
    trades: list[Trade] = field(default_factory=list)
    rejected: list[RejectedTrade] = field(default_factory=list)


def subscribe_messages(product_ids: Iterable[str]) -> list[str]:
    """Messages to send right after (re)connecting. Coinbase expects them within 5 seconds.

    ``heartbeats`` keeps the connection open when a product has no trades for a while.
    """
    return [
        json.dumps(
            {"type": "subscribe", "channel": "market_trades", "product_ids": list(product_ids)}
        ),
        json.dumps({"type": "subscribe", "channel": "heartbeats"}),
    ]


def parse_message(
    raw: str | bytes, *, exchange: str, ingest_time_ms: int, include_snapshots: bool = False
) -> ParsedMessage:
    """Parse one WebSocket message.

    Non-trade channels (heartbeats, subscriptions) yield an empty result. Each trade is
    validated on its own, so one bad trade doesn't discard the valid ones next to it.

    Snapshot events (the ~50 most recent trades, re-sent on every subscribe) are skipped
    unless ``include_snapshots`` is set: after a reconnect they would re-publish trades
    already produced, as very late events.

    Raises:
        TradeParseError: the message as a whole is malformed.
        CoinbaseSourceError: Coinbase reported an error.
    """
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise TradeParseError("invalid_json", str(exc)) from exc
    if not isinstance(payload, dict):
        raise TradeParseError(
            "invalid_shape", f"expected JSON object, got {type(payload).__name__}"
        )

    if payload.get("type") == "error":
        raise CoinbaseSourceError(payload.get("message") or json.dumps(payload))
    if payload.get("channel") != "market_trades":
        return ParsedMessage()

    events = payload.get("events")
    if not isinstance(events, list):
        raise TradeParseError("invalid_shape", "market_trades message has no events list")

    result = ParsedMessage()
    for event in events:
        if not isinstance(event, dict) or not isinstance(event.get("trades"), list):
            raise TradeParseError("invalid_shape", "market_trades event has no trades list")
        if event.get("type") == "snapshot" and not include_snapshots:
            continue
        for item in event["trades"]:
            try:
                trade = CoinbaseTrade.model_validate(item)
            except ValidationError as exc:
                result.rejected.append(
                    RejectedTrade(json.dumps(item), "validation_error", _summarize(exc))
                )
            else:
                result.trades.append(_to_trade(trade, exchange, ingest_time_ms))
    return result


def _to_trade(trade: CoinbaseTrade, exchange: str, ingest_time_ms: int) -> Trade:
    return Trade(
        exchange=exchange,
        symbol=trade.product_id.upper(),
        trade_id=trade.trade_id,
        price=trade.price,
        quantity=trade.size,
        is_buyer_maker=trade.side == "BUY",
        # Integer arithmetic avoids float rounding in timestamp() * 1000.
        event_time=(trade.time - _EPOCH) // _ONE_MS,
        ingest_time=ingest_time_ms,
    )


def _summarize(exc: ValidationError) -> str:
    """Compact one-line summary, e.g. ``price: Input should be greater than 0``."""
    return "; ".join(
        f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}" for err in exc.errors()
    )
