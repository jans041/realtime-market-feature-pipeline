from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import pytest

pytest.importorskip("great_expectations")
pytest.importorskip("pyiceberg")

from pipeline.governance.offline_checks import CheckResult, validate_table  # noqa: E402

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


def row(minutes_ago: int, **overrides: Any) -> dict[str, Any]:
    end = pd.Timestamp(NOW - timedelta(minutes=minutes_ago))
    return {
        "symbol": "BTC-USD",
        "window_start": end - pd.Timedelta(minutes=1),
        "window_end": end,
        "trade_count": 2,
        "volume": 4.0,
        "vwap": 107.5,
        "price_min": 100.0,
        "price_max": 110.0,
        "buy_volume": 1.0,
        "sell_volume": 3.0,
        **overrides,
    }


def table(*rows: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def by_rule(checks: list[CheckResult]) -> dict[str, CheckResult]:
    return {check.rule: check for check in checks}


def failed(checks: list[CheckResult]) -> set[str]:
    return {check.rule for check in checks if not check.success}


def healthy_table(n: int = 200) -> pd.DataFrame:
    return table(*(row(minutes_ago=i) for i in range(n)))


def test_healthy_table_passes_every_check() -> None:
    checks = validate_table(healthy_table(), NOW)

    assert failed(checks) == set()
    assert {"table_not_empty", "newest_window_is_fresh", "vwap_within_price_range"} <= set(
        by_rule(checks)
    )


def test_stale_table_fails_freshness() -> None:
    checks = validate_table(table(row(minutes_ago=30)), NOW)

    assert failed(checks) == {"newest_window_is_fresh"}


def test_a_few_replayed_rows_are_tolerated_but_many_are_not() -> None:
    healthy = healthy_table(200)
    one_replay = pd.concat([healthy, healthy.iloc[[0]]])  # 2 of 201 rows share a key
    many_replays = pd.concat([healthy, healthy.iloc[:20]])

    assert failed(validate_table(one_replay, NOW)) == set()
    assert failed(validate_table(many_replays, NOW)) == {"replay_duplicates_rare"}


def test_extreme_price_range_is_flagged_for_review() -> None:
    rows = healthy_table().to_dict("records") + [row(1, price_max=150.0, vwap=120.0)]

    checks = by_rule(validate_table(table(*rows), NOW))

    assert not checks["price_range_plausible"].success
    assert checks["price_range_plausible"].detail.startswith("1 of 201 rows")


def test_invariant_violations_in_the_table_are_reported() -> None:
    rows = healthy_table().to_dict("records") + [row(1, vwap=500.0)]

    assert "vwap_within_price_range" in failed(validate_table(table(*rows), NOW))
