"""Exponential backoff with full jitter for reconnect loops.

delay = uniform(0, min(cap, base * 2**attempt))

Jitter spreads reconnects out so many clients dropped at the same moment don't all
reconnect in lockstep (the "thundering herd").
See https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
"""

from __future__ import annotations

import random


class ExponentialBackoff:
    def __init__(
        self, base_seconds: float, cap_seconds: float, rng: random.Random | None = None
    ) -> None:
        if base_seconds <= 0 or cap_seconds < base_seconds:
            raise ValueError("require 0 < base_seconds <= cap_seconds")
        self._base = base_seconds
        self._cap = cap_seconds
        self._rng = rng or random.Random()
        self._attempt = 0

    @property
    def attempt(self) -> int:
        return self._attempt

    def ceiling(self) -> float:
        """Upper bound for the next delay, before jitter."""
        # Clamp the exponent so 2**attempt can't overflow on a very long outage.
        return min(self._cap, self._base * 2 ** min(self._attempt, 32))

    def next_delay(self) -> float:
        """Return the next delay in seconds and advance the attempt counter."""
        delay = self._rng.uniform(0, self.ceiling())
        self._attempt += 1
        return delay

    def reset(self) -> None:
        """Call once the connection is healthy again (e.g. after the first message)."""
        self._attempt = 0
