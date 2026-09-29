import json
from typing import Any

import pytest

from pipeline.ingestion.coinbase import (
    CoinbaseSourceError,
    ParsedMessage,
    parse_message,
    subscribe_messages,
)
from pipeline.ingestion.models import Trade, TradeParseError

INGEST_MS = 1_790_714_413_660


def cb_trade(**overrides: Any) -> dict[str, Any]:
    trade: dict[str, Any] = {
        "product_id": "BTC-USD",
        "trade_id": "1100154155",
        "price": "83592.48",
        "size": "0.00077733",
        "time": "2026-09-29T20:40:13.543036Z",
        "side": "SELL",
    }
    trade.update(overrides)
    return trade


def market_trades(*trades: dict[str, Any], event_type: str = "update") -> str:
    return json.dumps(
        {
            "channel": "market_trades",
            "timestamp": "2026-09-29T20:40:13.660022701Z",
            "sequence_num": 11,
            "events": [{"type": event_type, "trades": list(trades)}],
        }
    )


def parse(raw: str | bytes, **kwargs: Any) -> ParsedMessage:
    return parse_message(raw, exchange="coinbase", ingest_time_ms=INGEST_MS, **kwargs)


def test_parses_trade_update() -> None:
    parsed = parse(market_trades(cb_trade()))

    assert parsed.rejected == []
    assert parsed.trades == [
        Trade(
            exchange="coinbase",
            symbol="BTC-USD",
            trade_id=1_100_154_155,
            price=83592.48,
            quantity=0.00077733,
            is_buyer_maker=False,
            event_time=1_790_714_413_543,
            ingest_time=INGEST_MS,
        )
    ]


@pytest.mark.parametrize(("side", "is_buyer_maker"), [("BUY", True), ("SELL", False)])
def test_side_is_the_maker_side(side: str, is_buyer_maker: bool) -> None:
    [trade] = parse(market_trades(cb_trade(side=side))).trades

    assert trade.is_buyer_maker is is_buyer_maker


@pytest.mark.parametrize(
    ("time", "expected_ms"),
    [
        ("2026-09-29T20:40:13Z", 1_790_714_413_000),
        ("2026-09-29T20:40:13.5Z", 1_790_714_413_500),
        ("2026-09-29T20:40:13.999999Z", 1_790_714_413_999),  # truncated, not rounded up
    ],
)
def test_event_time_handles_variable_fraction_digits(time: str, expected_ms: int) -> None:
    [trade] = parse(market_trades(cb_trade(time=time))).trades

    assert trade.event_time == expected_ms


def test_multiple_trades_in_one_message() -> None:
    parsed = parse(
        market_trades(cb_trade(trade_id="1"), cb_trade(trade_id="2", product_id="ETH-USD"))
    )

    assert [(t.symbol, t.trade_id) for t in parsed.trades] == [("BTC-USD", 1), ("ETH-USD", 2)]


def test_bad_trade_is_rejected_without_dropping_its_neighbours() -> None:
    bad = cb_trade(trade_id="2", price="-1")
    parsed = parse(market_trades(cb_trade(trade_id="1"), bad, cb_trade(trade_id="3")))

    assert [t.trade_id for t in parsed.trades] == [1, 3]
    [rejected] = parsed.rejected
    assert json.loads(rejected.raw) == bad
    assert rejected.error_type == "validation_error"
    assert rejected.error_message.startswith("price: ")


@pytest.mark.parametrize(
    "overrides",
    [
        {"price": "abc"},
        {"price": "0"},
        {"size": "-0.5"},
        {"price": "NaN"},
        {"size": "inf"},
        {"trade_id": "not-a-number"},
        {"side": "HOLD"},
        {"product_id": ""},
        {"time": "yesterday"},
        {"time": "2026-09-29T20:40:13"},  # no timezone
    ],
)
def test_rejects_invalid_trade_fields(overrides: dict[str, Any]) -> None:
    parsed = parse(market_trades(cb_trade(**overrides)))

    assert parsed.trades == []
    assert len(parsed.rejected) == 1


def test_missing_field_is_rejected() -> None:
    trade = cb_trade()
    del trade["time"]

    assert len(parse(market_trades(trade)).rejected) == 1


def test_snapshots_skipped_by_default() -> None:
    raw = market_trades(cb_trade(), event_type="snapshot")

    assert parse(raw) == ParsedMessage()
    assert len(parse(raw, include_snapshots=True).trades) == 1


@pytest.mark.parametrize(
    "raw",
    [
        '{"channel":"heartbeats","events":[{"current_time":"...","heartbeat_counter":1}]}',
        '{"channel":"subscriptions","events":[{"subscriptions":{"market_trades":["BTC-USD"]}}]}',
    ],
)
def test_control_messages_are_ignored(raw: str) -> None:
    assert parse(raw) == ParsedMessage()


def test_source_error_is_raised_not_dead_lettered() -> None:
    with pytest.raises(CoinbaseSourceError, match="failure to subscribe"):
        parse('{"type":"error","message":"failure to subscribe"}')


@pytest.mark.parametrize(
    ("raw", "error_type"),
    [
        ("{not json", "invalid_json"),
        (b"\xff\xfe", "invalid_json"),
        ("[1, 2, 3]", "invalid_shape"),
        ('{"channel":"market_trades"}', "invalid_shape"),
        ('{"channel":"market_trades","events":[{"type":"update"}]}', "invalid_shape"),
        ('{"channel":"market_trades","events":["oops"]}', "invalid_shape"),
    ],
)
def test_malformed_message_raises(raw: str | bytes, error_type: str) -> None:
    with pytest.raises(TradeParseError) as exc_info:
        parse(raw)

    assert exc_info.value.error_type == error_type


def test_subscribe_messages() -> None:
    assert [json.loads(m) for m in subscribe_messages(["BTC-USD", "ETH-USD"])] == [
        {"type": "subscribe", "channel": "market_trades", "product_ids": ["BTC-USD", "ETH-USD"]},
        {"type": "subscribe", "channel": "heartbeats"},
    ]
