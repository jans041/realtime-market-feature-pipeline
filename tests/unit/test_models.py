import json

import fastavro

from pipeline.ingestion.config import REPO_ROOT
from pipeline.ingestion.models import Trade


def test_avro_dict_matches_registered_schema() -> None:
    """Guards against drift between the Trade dataclass and schemas/trade.avsc."""
    schema = json.loads((REPO_ROOT / "schemas" / "trade.avsc").read_text(encoding="utf-8"))
    record = Trade(
        exchange="coinbase",
        symbol="BTC-USD",
        trade_id=1_100_154_155,
        price=83592.48,
        quantity=0.00077733,
        is_buyer_maker=False,
        event_time=1_790_714_413_543,
        ingest_time=1_790_714_413_660,
    ).to_avro_dict()

    assert fastavro.validation.validate(record, fastavro.parse_schema(schema), strict=True)
    assert set(record) == {field["name"] for field in schema["fields"]}
