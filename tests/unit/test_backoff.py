import random

import pytest

from pipeline.ingestion.backoff import ExponentialBackoff


def test_ceiling_doubles_until_cap() -> None:
    backoff = ExponentialBackoff(base_seconds=1, cap_seconds=10, rng=random.Random(0))

    ceilings = []
    for _ in range(6):
        ceilings.append(backoff.ceiling())
        backoff.next_delay()

    assert ceilings == [1, 2, 4, 8, 10, 10]


def test_delays_are_jittered_within_ceiling() -> None:
    backoff = ExponentialBackoff(base_seconds=1, cap_seconds=60, rng=random.Random(42))

    for _ in range(50):
        ceiling = backoff.ceiling()
        assert 0 <= backoff.next_delay() <= ceiling


def test_same_seed_gives_same_sequence() -> None:
    a = ExponentialBackoff(1, 60, rng=random.Random(7))
    b = ExponentialBackoff(1, 60, rng=random.Random(7))

    assert [a.next_delay() for _ in range(5)] == [b.next_delay() for _ in range(5)]


def test_reset_starts_over() -> None:
    backoff = ExponentialBackoff(1, 60, rng=random.Random(0))
    for _ in range(4):
        backoff.next_delay()

    backoff.reset()

    assert backoff.attempt == 0
    assert backoff.ceiling() == 1


def test_long_outage_does_not_overflow() -> None:
    backoff = ExponentialBackoff(1, 60, rng=random.Random(0))
    for _ in range(5000):
        backoff.next_delay()

    assert backoff.ceiling() == 60


@pytest.mark.parametrize(("base", "cap"), [(0, 10), (-1, 10), (5, 1)])
def test_rejects_invalid_bounds(base: float, cap: float) -> None:
    with pytest.raises(ValueError):
        ExponentialBackoff(base, cap)
