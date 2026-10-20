"""Run the compatibility check against a real Schema Registry.

These also prove the CI gate isn't vacuous: a check that passed everything would fail the
"breaking change" cases here.

Needs: docker compose up -d --wait schema-registry      Run: pytest -m integration
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from pipeline.governance.schema_compat import check_compatibility

pytestmark = pytest.mark.integration

REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TRADE_SCHEMA = Path(__file__).parents[2] / "schemas" / "trade.avsc"

Schema = dict[str, Any]


def _trade() -> Schema:
    schema: Schema = json.loads(TRADE_SCHEMA.read_text(encoding="utf-8"))
    return schema


def _add_optional_field(schema: Schema) -> None:
    schema["fields"].append({"name": "venue", "type": ["null", "string"], "default": None})


def _add_required_field(schema: Schema) -> None:
    schema["fields"].append({"name": "venue", "type": "string"})


def _remove_field(schema: Schema) -> None:
    schema["fields"] = [f for f in schema["fields"] if f["name"] != "ingest_time"]


def _narrow_price_to_string(schema: Schema) -> None:
    next(f for f in schema["fields"] if f["name"] == "price")["type"] = "string"


def _widen_trade_id_to_double(schema: Schema) -> None:
    # Avro promotes long -> double, so readers using the new schema can read old longs.
    next(f for f in schema["fields"] if f["name"] == "trade_id")["type"] = "double"


def _check(change: Callable[[Schema], None]) -> tuple[bool, list[str]]:
    candidate = _trade()
    change(candidate)
    result = check_compatibility(REGISTRY_URL, json.dumps(_trade()), json.dumps(candidate))
    return result.compatible, result.messages


def test_unchanged_schema_is_compatible() -> None:
    assert _check(lambda schema: None) == (True, [])


@pytest.mark.parametrize(
    "change",
    [_add_optional_field, _remove_field, _widen_trade_id_to_double],
    ids=lambda change: change.__name__.lstrip("_"),
)
def test_backward_compatible_changes_pass(change: Callable[[Schema], None]) -> None:
    compatible, messages = _check(change)

    assert compatible, messages


@pytest.mark.parametrize(
    "change",
    [_add_required_field, _narrow_price_to_string],
    ids=lambda change: change.__name__.lstrip("_"),
)
def test_breaking_changes_fail_with_a_reason(change: Callable[[Schema], None]) -> None:
    compatible, messages = _check(change)

    assert not compatible
    assert messages, "the registry should explain why the change is incompatible"
