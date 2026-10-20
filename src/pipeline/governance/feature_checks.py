"""Data quality rules for 1-minute trade features, as a Great Expectations suite.

Two layers guard the feature store:
  - pydantic (``FeatureRow`` in features/sink.py): per-message structure, i.e. types,
    required fields, positive prices. Cheap, and rejects garbage before anything else runs.
  - this suite: rules that relate fields to each other, e.g. the VWAP must lie between the
    window's min and max price. GX reports each rule separately, with the failing rows.

Only *invariants* live here: rules that hold for every correct feature row by construction,
so a failure always means a bug or corruption upstream, and the sink can safely send the
row to the DLQ. Heuristics (e.g. "price moved < 20% in a minute") would also drop real
flash-crash data, so they belong in offline monitoring, not in the write path.

Some rules compare derived columns (computed in ``prepare``) against a tolerance, because
GX's built-in expectations can't do cross-column arithmetic on pandas data, and exact float
comparisons would flag rounding noise: a VWAP of identical prices can come out 1 ulp above
the max price.

The suite is built in code on an ephemeral (in-memory) context: no great_expectations/
project directory or YAML to keep in sync, and it is reviewed and versioned like any code.
"""

from __future__ import annotations

from typing import Any

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

WINDOW_SECONDS = 60
SYMBOL_PATTERN = r"^[A-Z0-9]+-[A-Z0-9]+$"  # BASE-QUOTE, e.g. BTC-USD
# Relative tolerance for float sums and averages: far above rounding error (~1e-16),
# far below any real discrepancy.
RELATIVE_TOLERANCE = 1e-9

FEATURE_COLUMNS = (
    "symbol",
    "window_start",
    "window_end",
    "trade_count",
    "volume",
    "vwap",
    "price_min",
    "price_max",
    "buy_volume",
    "sell_volume",
)


def prepare(features: pd.DataFrame) -> pd.DataFrame:
    """Add the derived columns the rules check. Returns a copy with a 0..n-1 index,
    so the row numbers GX reports are positions in ``features``."""
    frame = features.reset_index(drop=True)
    frame["window_seconds"] = (frame["window_end"] - frame["window_start"]).dt.total_seconds()
    # How far the VWAP is outside [price_min, price_max], relative to the VWAP (0 if inside).
    outside = (frame["price_min"] - frame["vwap"]).clip(lower=0) + (
        frame["vwap"] - frame["price_max"]
    ).clip(lower=0)
    frame["vwap_outside_range"] = outside / frame["vwap"]
    frame["side_volume_mismatch"] = (
        frame["buy_volume"] + frame["sell_volume"] - frame["volume"]
    ).abs() / frame["volume"]
    return frame


def _rule(name: str, expectation: gxe.Expectation) -> gxe.Expectation:
    # The rule name travels in ``meta`` so results can be reported by name.
    expectation.meta = {"rule": name}
    return expectation


def invariant_expectations() -> list[gxe.Expectation]:
    """Row-level rules; each failing row is reported, so it can be dead-lettered."""
    return [
        _rule(
            "symbol_format",
            gxe.ExpectColumnValuesToMatchRegex(column="symbol", regex=SYMBOL_PATTERN),
        ),
        _rule(
            "window_is_one_minute",
            gxe.ExpectColumnValuesToBeInSet(column="window_seconds", value_set=[WINDOW_SECONDS]),
        ),
        _rule(
            "price_min_not_above_max",
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A="price_max", column_B="price_min", or_equal=True
            ),
        ),
        _rule(
            "vwap_within_price_range",
            gxe.ExpectColumnValuesToBeBetween(
                column="vwap_outside_range", min_value=0, max_value=RELATIVE_TOLERANCE
            ),
        ),
        _rule(
            "buy_plus_sell_equals_volume",
            gxe.ExpectColumnValuesToBeBetween(
                column="side_volume_mismatch", min_value=0, max_value=RELATIVE_TOLERANCE
            ),
        ),
    ]


def rule_name(result: Any) -> str:
    meta = result.expectation_config.meta or {}
    return str(meta.get("rule", result.expectation_config.type))


class RuleError(RuntimeError):
    """A rule couldn't be evaluated (e.g. a missing column): a bug, not bad data."""


def raise_if_errored(result: Any) -> None:
    # GX puts errors either at the top level or nested under the failing metric's id.
    info = result.exception_info or {}
    entries = [info, *(v for v in info.values() if isinstance(v, dict))]
    for entry in entries:
        if entry.get("raised_exception"):
            raise RuleError(f"Rule {rule_name(result)} errored: {entry.get('exception_message')}")


class FeatureValidator:
    """Validates batches of feature rows against the invariant suite.

    Build once and reuse: creating the context and suite is the slow part, validating a
    batch of a few hundred rows takes ~20 ms.
    """

    def __init__(self) -> None:
        context = gx.get_context(mode="ephemeral")
        asset = context.data_sources.add_pandas("features").add_dataframe_asset("batch")
        self._batch_definition = asset.add_batch_definition_whole_dataframe("whole_batch")
        self._suite = gx.ExpectationSuite(
            name="trade_features_1m_invariants", expectations=invariant_expectations()
        )

    def failures(self, features: pd.DataFrame) -> dict[int, list[str]]:
        """Row position in ``features`` -> names of the rules it broke (only failing rows)."""
        if features.empty:
            return {}
        batch = self._batch_definition.get_batch(batch_parameters={"dataframe": prepare(features)})
        # COMPLETE includes every failing row's index, not just a sample.
        results = batch.validate(self._suite, result_format={"result_format": "COMPLETE"})
        failed: dict[int, list[str]] = {}
        for result in results.results:
            if result.success:
                continue
            # Don't dead-letter every row because a rule is broken: fail loudly instead.
            raise_if_errored(result)
            for position in result.result.get("unexpected_index_list", []):
                failed.setdefault(int(position), []).append(rule_name(result))
        return failed
