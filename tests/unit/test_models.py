import json
from typing import Any

import fastavro
import pytest

from pipeline.ingestion.config import REPO_ROOT
from pipeline.ingestion.models import Trade, TradeParseError, parse_trade

INGEST_MS = 1_727_600_000_500


def binance_event(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "e": "trade",
        "E": 1_727_600_000_100,
        "s": "BTCUSDT",
        "t": 12345,
        "p": "65000.10",
        "q": "0.00210000",
        "T": 1_727_600_000_000,
        "m": True,
        "M": True,
    }
    event.update(overrides)
    return event


def parse(payload: Any) -> Trade:
    raw = payload if isinstance(payload, str | bytes) else json.dumps(payload)
    return parse_trade(raw, exchange="binance_us", ingest_time_ms=INGEST_MS)


def test_parses_raw_stream_event() -> None:
    trade = parse(binance_event())

    assert trade == Trade(
        exchange="binance_us",
        symbol="BTCUSDT",
        trade_id=12345,
        price=65000.10,
        quantity=0.0021,
        is_buyer_maker=True,
        event_time=1_727_600_000_000,
        ingest_time=INGEST_MS,
    )


def test_unwraps_combined_stream_envelope() -> None:
    trade = parse({"stream": "ethusdt@trade", "data": binance_event(s="ETHUSDT")})

    assert trade.symbol == "ETHUSDT"


def test_accepts_bytes_and_normalizes_symbol_case() -> None:
    trade = parse(json.dumps(binance_event(s="btcusdt")).encode())

    assert trade.symbol == "BTCUSDT"


@pytest.mark.parametrize(
    ("payload", "error_type"),
    [
        ("{not json", "invalid_json"),
        (b"\xff\xfe", "invalid_json"),
        ("[1, 2, 3]", "invalid_shape"),
        ({"stream": "x", "data": "not-an-object"}, "invalid_shape"),
        (binance_event(e="aggTrade"), "validation_error"),
        (binance_event(p="abc"), "validation_error"),
        (binance_event(p="0"), "validation_error"),
        (binance_event(q="-1.5"), "validation_error"),
        (binance_event(p="NaN"), "validation_error"),
        (binance_event(q="inf"), "validation_error"),
        (binance_event(t=-1), "validation_error"),
        (binance_event(s=""), "validation_error"),
        ({k: v for k, v in binance_event().items() if k != "T"}, "validation_error"),
    ],
)
def test_rejects_invalid_payloads(payload: Any, error_type: str) -> None:
    with pytest.raises(TradeParseError) as exc_info:
        parse(payload)

    assert exc_info.value.error_type == error_type
    assert str(exc_info.value)


def test_validation_error_message_names_the_field() -> None:
    with pytest.raises(TradeParseError, match=r"^p: "):
        parse(binance_event(p="0"))


def test_avro_dict_matches_registered_schema() -> None:
    """Guards against drift between the Trade dataclass and schemas/trade.avsc."""
    schema = json.loads((REPO_ROOT / "schemas" / "trade.avsc").read_text(encoding="utf-8"))
    record = parse(binance_event()).to_avro_dict()

    assert fastavro.validation.validate(record, fastavro.parse_schema(schema), strict=True)
    assert set(record) == {field["name"] for field in schema["fields"]}
