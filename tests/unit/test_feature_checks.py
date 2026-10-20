from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

pytest.importorskip("great_expectations")

from pipeline.governance.feature_checks import (  # noqa: E402
    FeatureValidator,
    RuleError,
    invariant_expectations,
    raise_if_errored,
)

GOOD: dict[str, Any] = {
    "symbol": "BTC-USD",
    "window_start": pd.Timestamp("2026-10-05T12:00:00Z"),
    "window_end": pd.Timestamp("2026-10-05T12:01:00Z"),
    "trade_count": 2,
    "volume": 4.0,
    "vwap": 107.5,
    "price_min": 100.0,
    "price_max": 110.0,
    "buy_volume": 1.0,
    "sell_volume": 3.0,
}


def frame(*overrides: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame([{**GOOD, **o} for o in overrides])


@pytest.fixture(scope="module")
def validator() -> Iterator[FeatureValidator]:
    yield FeatureValidator()  # slow to build, so shared by the module


def test_valid_rows_pass(validator: FeatureValidator) -> None:
    assert validator.failures(frame({}, {"symbol": "ETH-USD"})) == {}


def test_empty_batch_passes(validator: FeatureValidator) -> None:
    assert validator.failures(frame().iloc[0:0]) == {}


@pytest.mark.parametrize(
    ("override", "rules"),
    [
        ({"symbol": "btcusd"}, ["symbol_format"]),
        ({"window_end": pd.Timestamp("2026-10-05T12:00:30Z")}, ["window_is_one_minute"]),
        # With min > max no VWAP can be in range, so both rules fire.
        (
            {"price_min": 105.0, "price_max": 104.0, "vwap": 104.5},
            ["price_min_not_above_max", "vwap_within_price_range"],
        ),
        ({"vwap": 99.0}, ["vwap_within_price_range"]),
        ({"vwap": 110.5}, ["vwap_within_price_range"]),
        ({"sell_volume": 2.0}, ["buy_plus_sell_equals_volume"]),
    ],
)
def test_each_rule_catches_its_violation(
    validator: FeatureValidator, override: dict[str, Any], rules: list[str]
) -> None:
    assert validator.failures(frame({}, override)) == {1: rules}


def test_float_rounding_noise_is_not_a_violation(validator: FeatureValidator) -> None:
    # sum(p*q)/sum(q) over identical prices can land 1 ulp above the max price.
    price = 83592.48
    rows = frame(
        {
            "vwap": price * (1 + 2e-16),
            "price_min": price,
            "price_max": price,
            "volume": 0.1 + 0.2,
            "buy_volume": 0.1,
            "sell_volume": 0.2,
        }
    )

    assert validator.failures(rows) == {}


def test_reports_positions_in_the_input_and_all_broken_rules(
    validator: FeatureValidator,
) -> None:
    # Index labels deliberately not 0..n-1: results are positions, not labels.
    rows = frame({}, {"symbol": "bad", "sell_volume": 9.0}, {}).set_axis([10, 20, 30])

    assert validator.failures(rows) == {1: ["symbol_format", "buy_plus_sell_equals_volume"]}


def test_missing_column_fails_loudly(validator: FeatureValidator) -> None:
    with pytest.raises(KeyError):
        validator.failures(frame({}).drop(columns="price_max"))


@pytest.mark.parametrize(
    "exception_info",
    [
        {"raised_exception": True, "exception_message": "boom"},
        # Shape GX uses when a metric fails: nested under the metric's id.
        {"MetricConfigurationID(...)": {"raised_exception": True, "exception_message": "boom"}},
    ],
)
def test_a_rule_that_errored_raises_instead_of_rejecting_rows(
    exception_info: dict[str, Any],
) -> None:
    result = SimpleNamespace(
        exception_info=exception_info,
        expectation_config=SimpleNamespace(meta={"rule": "vwap_within_price_range"}, type="t"),
    )

    with pytest.raises(RuleError, match="vwap_within_price_range errored: boom"):
        raise_if_errored(result)


def test_a_failed_rule_without_an_error_does_not_raise() -> None:
    result = SimpleNamespace(
        exception_info={"raised_exception": False, "exception_message": None},
        expectation_config=SimpleNamespace(meta={"rule": "r"}, type="t"),
    )

    raise_if_errored(result)


def test_every_rule_has_a_unique_name() -> None:
    names = [e.meta["rule"] for e in invariant_expectations() if e.meta]

    assert len(names) == len(set(names)) == len(invariant_expectations())
