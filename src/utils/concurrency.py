"""Async concurrency utilities for scrapers."""

import asyncio
import functools
from typing import Any, TypeVar
from collections.abc import Callable

from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate

T = TypeVar("T")


class RateLimiter:
    """Async rate limiter backed by PyrateLimiter.

    Supports both integer and fractional rates (e.g. ``0.33`` = 1 request
    every ~3 seconds).

    Uses ``Rate(1, interval)`` (not ``Rate(rps, Duration.SECOND)``) to enforce
    strict even spacing between requests with no burst allowed. This prevents
    bursty traffic that can trigger rate limiting on target servers.

    Args:
        requests_per_second: Maximum request rate. Values < 1 are supported.
    """

    def __init__(self, requests_per_second: float):
        self.requests_per_second = requests_per_second
        # Rate(1, interval) ensures strict 1/rps spacing — no burst allowed.
        # Rate(rps, Duration.SECOND) would allow rps requests to fire instantly.
        interval = Duration.SECOND * (1 / requests_per_second)
        rate = Rate(1, interval)
        self._limiter = Limiter(InMemoryBucket([rate]))

    async def acquire(self) -> None:
        """Block until a request slot is available."""
        await self._limiter.try_acquire_async("req")


async def run_in_thread(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a sync function in a thread via asyncio.to_thread."""
    if kwargs:
        partial = functools.partial(func, *args, **kwargs)
        return await asyncio.to_thread(partial)
    return await asyncio.to_thread(func, *args)
