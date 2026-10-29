"""One lazy, bounded Chromium page pool per scraper."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page, async_playwright
from playwright_stealth import Stealth
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.core.http import RateLimiter

_ERROR_MARKERS = (
    b"Azion - Default error page",
    b"<title>403 Forbidden</title>",
    b"<title>Access Denied</title>",
    b"<title>Error</title>",
    b"Acesso Proibido",
    b"Erro 403",
    b"error/error.css",
    b"The website encountered an unexpected error",
)


class BrowserPool:
    def __init__(
        self, max_pages: int = 4, *, limiter: RateLimiter, max_retries: int = 3
    ) -> None:
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        self.max_pages = max_pages
        self.limiter = limiter
        self.max_retries = max_retries
        self._sem = asyncio.Semaphore(max_pages)
        self._lock = asyncio.Lock()
        self._idle: list[Page] = []
        self._context = None
        self._stack = AsyncExitStack()

    async def _initialize(self) -> None:
        if self._context is not None:
            return
        stack = AsyncExitStack()
        try:
            playwright = await stack.enter_async_context(async_playwright())
            Stealth().hook_playwright_context(playwright)
            browser = await playwright.chromium.launch(headless=True)
            stack.push_async_callback(browser.close)
            context = await browser.new_context(ignore_https_errors=True)
            stack.push_async_callback(context.close)
            self._context = context
            self._stack = stack.pop_all()
        finally:
            await stack.aclose()

    @asynccontextmanager
    async def page(self) -> AsyncIterator[Page]:
        async with self._sem:
            async with self._lock:
                await self._initialize()
                page = self._idle.pop() if self._idle else None
                if page is None or page.is_closed():
                    page = await self._context.new_page()
            try:
                yield page
            finally:
                if not page.is_closed():
                    self._idle.append(page)

    async def goto(self, page: Page, url: str, **kwargs: Any) -> None:
        await self.limiter.acquire()
        response = await page.goto(url, **kwargs)
        if response is not None and response.status >= 400:
            raise RuntimeError(f"Browser HTTP {response.status}: {url}")

    async def capture(self, page: Page) -> bytes:
        session = await page.context.new_cdp_session(page)
        try:
            snapshot = await session.send("Page.captureSnapshot", {"format": "mhtml"})
            return snapshot["data"].encode("utf-8")
        finally:
            await session.detach()

    async def snapshot(
        self,
        url: str,
        *,
        wait_until: str = "load",
        wait_for_selector: str | None = None,
        timeout: int = 60000,
    ) -> tuple[BeautifulSoup, bytes]:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(min=1, max=10),
            retry=retry_if_exception_type((PlaywrightError, RuntimeError)),
            reraise=True,
        ):
            with attempt:
                async with self.page() as page:
                    await self.goto(page, url, wait_until=wait_until, timeout=timeout)
                    if wait_for_selector:
                        await page.wait_for_selector(wait_for_selector, timeout=timeout)
                    html = await page.content()
                    mhtml = await self.capture(page)
                    if any((marker in mhtml[:8192] for marker in _ERROR_MARKERS)):
                        raise RuntimeError(f"Browser returned an error page: {url}")
                    return (BeautifulSoup(html, "lxml"), mhtml)
        raise AssertionError("Retry loop did not execute")

    async def soup(self, url: str) -> BeautifulSoup:
        soup, _ = await self.snapshot(url)
        return soup

    async def close(self) -> None:
        try:
            await self._stack.aclose()
        finally:
            self._context = None
            self._idle.clear()
