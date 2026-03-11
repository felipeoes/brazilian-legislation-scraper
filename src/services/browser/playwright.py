"""Playwright browser service for async web scraping."""

import asyncio
from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
)
from playwright_stealth import Stealth


class BrowserService:
    """Manages a Playwright browser instance and page pool.

    Encapsulates all Playwright lifecycle (launch, page management, teardown)
    so that scrapers interact with a clean, high-level API.

    Args:
        multiple_pages: If True, pre-create *max_workers* pages in a pool.
        max_workers: Number of pages to open when *multiple_pages* is True.
        verbose: Enable verbose logging.
        owner_class_name: Optional label for diagnostics/logging.
    """

    def __init__(
        self,
        multiple_pages: bool = False,
        max_workers: int = 50,
        headless: bool = True,
        verbose: bool = False,
        owner_class_name: str = "browser",
    ) -> None:
        self.multiple_pages = multiple_pages
        self.max_workers = max_workers
        self.headless = headless
        self.verbose = verbose
        self.owner_class_name = owner_class_name

        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._browser_context: BrowserContext | None = None
        self.page: Page | None = None
        self.pages: list[Page] = []
        self._page_pool: asyncio.Queue[Page] = asyncio.Queue()

    async def _init_pages(self) -> None:
        """Create the page pool or single page from the current browser context."""
        if not self._browser_context:
            raise RuntimeError("Browser context is not initialized.")

        if self.multiple_pages:
            self.pages = await asyncio.gather(
                *[self._browser_context.new_page() for _ in range(self.max_workers)]
            )
            for page in self.pages:
                self._page_pool.put_nowait(page)
            if self.verbose:
                logger.info(f"{self.max_workers} pages initialized in parallel")
        else:
            # Reuse existing page if available (e.g. persistent contexts)
            if self._browser_context.pages:
                self.page = self._browser_context.pages[0]
            else:
                self.page = await self._browser_context.new_page()

    async def initialize(self) -> None:
        """Launch Playwright and open the browser / page pool."""
        self._playwright = await async_playwright().start()
        Stealth().hook_playwright_context(self._playwright)

        launch_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ]
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless, args=launch_args
        )
        self._browser_context = await self._browser.new_context(accept_downloads=True)

        await self._browser_context.grant_permissions(
            ["clipboard-read", "clipboard-write"]
        )
        await self._init_pages()

    async def get_soup(self, url: str, page: Page | None = None) -> BeautifulSoup:
        """Navigate to *url* and return parsed HTML as BeautifulSoup."""
        target_page = page or self.page
        if target_page is None:
            raise RuntimeError("Playwright page is not initialized.")
        await target_page.goto(url, wait_until="domcontentloaded")
        content = await target_page.content()
        return BeautifulSoup(content, "html.parser")

    async def capture_mhtml(
        self,
        url: str,
        page: Page | None = None,
        *,
        wait_until: str = "load",
        timeout: int = 120_000,
    ) -> bytes:
        """Navigate to *url* and return the page as an MHTML snapshot.

        Uses Chrome DevTools Protocol (CDP) ``Page.captureSnapshot`` to
        produce a self-contained MHTML archive that includes all external
        resources (CSS, images, fonts, etc.).
        """
        _, mhtml = await self.fetch_and_capture(
            url, page=page, wait_until=wait_until, timeout=timeout
        )
        return mhtml

    async def fetch_and_capture(
        self,
        url: str,
        page: Page | None = None,
        *,
        wait_until: str = "load",
        timeout: int = 120_000,
        wait_for_selector: str | None = None,
    ) -> tuple[str, bytes]:
        """Navigate to *url* once, return ``(html_content, mhtml_bytes)``.

        Performs a single navigation and extracts both the rendered HTML
        (via ``page.content()``) and a self-contained MHTML archive (via
        CDP ``Page.captureSnapshot``).

        If *wait_for_selector* is given, waits for that CSS selector to
        appear in the DOM after navigation (useful for SPA pages that
        render content asynchronously via JS).
        """
        target_page = page or self.page
        if target_page is None:
            raise RuntimeError("Playwright page is not initialized.")
        await target_page.goto(url, wait_until=wait_until, timeout=timeout)
        if wait_for_selector:
            await target_page.wait_for_selector(wait_for_selector, timeout=timeout)
        html_content = await target_page.content()
        client = await target_page.context.new_cdp_session(target_page)
        snapshot = await client.send("Page.captureSnapshot")
        await client.detach()
        return html_content, snapshot["data"].encode("utf-8")

    async def get_available_page(self) -> Page:
        """Return the next available page from the pool (blocks until one is free).

        Raises RuntimeError if no page becomes available within 180 seconds, to
        prevent silent deadlocks when a page is never released back to the pool.
        Replaces closed pages transparently before returning.
        """
        try:
            page = await asyncio.wait_for(self._page_pool.get(), timeout=180.0)
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"{self.owner_class_name}: timed out waiting for an available page "
                "after 180 s — a page may have been acquired but never released"
            )
        if page.is_closed():
            page = await self._browser_context.new_page()
        return page

    def release_page(self, page: Page) -> None:
        """Return *page* to the pool for reuse."""
        self._page_pool.put_nowait(page)

    async def cleanup(self) -> None:
        """Close all pages, the browser, and stop Playwright."""
        for p in self.pages:
            try:
                await p.close()
            except Exception as e:
                logger.debug(f"Error closing page: {e}")
        self.pages.clear()
        if self.page:
            try:
                await self.page.close()
            except Exception as e:
                logger.debug(f"Error closing single page: {e}")
            self.page = None
        if self._browser_context:
            try:
                await self._browser_context.close()
            except Exception as e:
                logger.debug(f"Error closing browser context: {e}")
            self._browser_context = None
        if self._browser:
            try:
                await self._browser.close()
            except Exception as e:
                logger.debug(f"Error closing browser: {e}")
            self._browser = None
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception as e:
                logger.debug(f"Error stopping Playwright: {e}")
            self._playwright = None
