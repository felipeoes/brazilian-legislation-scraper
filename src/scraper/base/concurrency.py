"""Async concurrency utilities for scrapers."""

import asyncio
import functools
from typing import Any, TypeVar
from collections.abc import Callable

T = TypeVar("T")


class RateLimiter:
    """Sliding-window rate limiter for async API calls.

    Supports both integer and fractional rates (e.g. ``0.33`` = 1 request
    every ~3 seconds).

    Args:
        requests_per_second: Maximum request rate. Values < 1 are supported.
    """

    def __init__(self, requests_per_second: float):
        self.requests_per_second = requests_per_second
        # Capacity is capped at 1 so the bucket never holds more than one token.
        # This enforces strict 1/rps spacing between requests — no burst allowed.
        # (A capacity > 1 would let burst-many requests fire simultaneously on the
        # first call, which is undesirable for API rate limiters.)
        self._capacity = 1.0
        self._tokens = self._capacity
        # Lazily initialised on first acquire() call, when a running loop is guaranteed.
        self._last_updated: float | None = None
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until a request slot (token) is available.

        Uses a Token Bucket algorithm to guarantee mathematically precise
        rate limiting without maintaining arrays of future timestamps that
        can starve under high asyncio sleep contention.
        """
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()

            if self._last_updated is None:
                # First call: initialise the clock without consuming any tokens.
                self._last_updated = now

            elapsed = now - self._last_updated

            # Replenish tokens based on time elapsed
            self._tokens = min(
                self._capacity, self._tokens + elapsed * self.requests_per_second
            )
            self._last_updated = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                sleep_time = 0.0
            else:
                # Pre-order the next available slot: calculate how long until a full
                # token accrues from the current balance, then advance the clock so
                # the next waiter computes its delay correctly.
                tokens_needed = 1.0 - self._tokens
                sleep_time = tokens_needed / self.requests_per_second
                self._last_updated = now + sleep_time
                self._tokens = 0.0

        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


async def run_in_thread(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a sync function in a thread via asyncio.to_thread."""
    if kwargs:
        partial = functools.partial(func, *args, **kwargs)
        return await asyncio.to_thread(partial)
    return await asyncio.to_thread(func, *args)
