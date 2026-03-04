"""Async concurrency utilities for scrapers."""

import asyncio
import functools
from typing import Any, TypeVar
from collections.abc import Awaitable, Callable, Sequence

from loguru import logger
from tqdm.auto import tqdm

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


async def bounded_gather(
    coros: Sequence[Awaitable[T]],
    max_concurrency: int,
    desc: str = "",
    verbose: bool = False,
    show_progress: bool = True,
) -> list[T]:
    """Run awaitables with bounded concurrency via a semaphore.

    Args:
        coros: Sequence of awaitables to run.
        max_concurrency: Maximum number of concurrent tasks.
        desc: Description for logging / tqdm bar.
        verbose: Whether to log detailed progress messages.
        show_progress: Whether to show a tqdm progress bar (always on by default).

    Returns:
        List of results in the same order as input coros.
    """
    semaphore = asyncio.Semaphore(max_concurrency)
    total = len(coros)
    pbar = tqdm(total=total, desc=desc, disable=not show_progress)

    async def _limited(idx: int, coro: Awaitable[T]) -> tuple[int, T]:
        async with semaphore:
            result = await coro
            pbar.update(1)
            if verbose and total > 0 and pbar.n % max(1, total // 10) == 0:
                logger.info(f"{desc} | Progress: {pbar.n}/{total}")
            return idx, result

    tasks = [asyncio.create_task(_limited(i, c)) for i, c in enumerate(coros)]
    indexed_results = await asyncio.gather(*tasks, return_exceptions=True)
    pbar.close()

    results: list[Any] = [None] * total
    for item in indexed_results:
        if isinstance(item, BaseException):
            logger.error(f"{desc} | Task failed: {item}")
            continue
        idx, result = item
        results[idx] = result

    return results


async def run_in_thread(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a sync function in a thread via asyncio.to_thread."""
    if kwargs:
        partial = functools.partial(func, *args, **kwargs)
        return await asyncio.to_thread(partial)
    return await asyncio.to_thread(func, *args)
