"""Validate the offline feature table (Iceberg) with Great Expectations.

The sink already rejects rows that break an invariant, so this is monitoring, not a gate:
it checks properties of the *table* that no single batch can show, and reports them.

  - invariants again: a failure means rows reached the table without passing the sink
  - not empty, and fresh: the newest window is recent, otherwise the pipeline has stalled
  - replay rate: duplicate (symbol, window_end) rows come from at-least-once redelivery.
    A few are expected (Feast keeps the latest ``ingested_at``); many mean the sink is
    crash-looping or failing to commit offsets
  - plausibility: a heuristic flagging windows with an extreme price range for a human
    to look at. Not an invariant, so the sink never rejects on it

Exits 1 if any check fails, so it can run from cron or a scheduled job.

Run:  python -m pipeline.governance.offline_checks --hours 24
      docker compose run --rm feature-sink python -m pipeline.governance.offline_checks
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from pyiceberg.table import Table

from pipeline.features.config import FeatureStoreSettings
from pipeline.features.iceberg import open_catalog
from pipeline.governance.feature_checks import (
    invariant_expectations,
    named_rule,
    new_batch_definition,
    prepare,
    raise_if_errored,
    rule_name,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Thresholds:
    max_staleness: timedelta = timedelta(minutes=10)
    # Fraction of rows sharing (symbol, window_end) with another row. Both rows of a
    # duplicated pair count, so 0.01 is roughly one replayed row per 200.
    max_duplicate_fraction: float = 0.01
    # (max - min) / min within one minute. BTC rarely moves 1% in a minute; 20% would be
    # a flash crash or a bad print, either way worth a look.
    max_price_range: float = 0.20


@dataclass(frozen=True)
class CheckResult:
    rule: str
    success: bool
    detail: str


def table_expectations(now: datetime, thresholds: Thresholds) -> list[gxe.Expectation]:
    return [
        *invariant_expectations(),
        named_rule("table_not_empty", gxe.ExpectTableRowCountToBeBetween(min_value=1)),
        named_rule(
            "newest_window_is_fresh",
            gxe.ExpectColumnMaxToBeBetween(
                column="window_end", min_value=now - thresholds.max_staleness
            ),
        ),
        named_rule(
            "replay_duplicates_rare",
            gxe.ExpectCompoundColumnsToBeUnique(
                column_list=["symbol", "window_end"],
                mostly=1 - thresholds.max_duplicate_fraction,
            ),
        ),
        named_rule(
            "price_range_plausible",
            gxe.ExpectColumnValuesToBeBetween(
                column="price_range", min_value=0, max_value=thresholds.max_price_range
            ),
        ),
    ]


def _detail(result: Any) -> str:
    r = result.result
    if "observed_value" in r:
        return f"observed {r['observed_value']}"
    if "unexpected_count" in r:
        return (
            f"{r['unexpected_count']} of {r.get('element_count', '?')} rows "
            f"({r.get('unexpected_percent') or 0:.2f}%) unexpected"
        )
    return ""


def validate_table(
    features: pd.DataFrame, now: datetime, thresholds: Thresholds | None = None
) -> list[CheckResult]:
    thresholds = thresholds or Thresholds()
    frame = prepare(features)
    frame["price_range"] = (frame["price_max"] - frame["price_min"]) / frame["price_min"]
    suite = gx.ExpectationSuite(
        name="trade_features_1m_table", expectations=table_expectations(now, thresholds)
    )
    batch = new_batch_definition().get_batch(batch_parameters={"dataframe": frame})
    results = batch.validate(suite, result_format={"result_format": "SUMMARY"})
    checks = []
    for result in results.results:
        if not result.success:
            raise_if_errored(result)
        checks.append(CheckResult(rule_name(result), bool(result.success), _detail(result)))
    return checks


def load_recent(table: Table, since: datetime) -> pd.DataFrame:
    """Rows whose window ended at or after ``since``. The table is partitioned by day of
    ``window_end``, so the filter also prunes whole days of files."""
    scan = table.scan(row_filter=f"window_end >= '{since.isoformat()}'")
    return scan.to_pandas()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hours", type=float, default=24.0, help="how far back to check")
    parser.add_argument("--max-staleness-minutes", type=float, default=10.0)
    args = parser.parse_args(argv)

    settings = FeatureStoreSettings()  # type: ignore[call-arg]  # S3 credentials come from env
    logging.basicConfig(level=settings.log_level, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("great_expectations").setLevel(logging.WARNING)  # setup chatter

    now = datetime.now(UTC)
    table = open_catalog(settings).load_table(settings.table_identifier)
    features = load_recent(table, now - timedelta(hours=args.hours))
    logger.info(
        "Checking %d rows of %s from the last %.0f h",
        len(features),
        settings.table_identifier,
        args.hours,
    )

    checks = validate_table(
        features, now, Thresholds(max_staleness=timedelta(minutes=args.max_staleness_minutes))
    )
    for check in checks:
        log = logger.info if check.success else logger.error
        log("%-4s %-28s %s", "PASS" if check.success else "FAIL", check.rule, check.detail)
    failed = sum(not c.success for c in checks)
    logger.info("%d of %d checks passed", len(checks) - failed, len(checks))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
