"""Playwright browser service for async web scraping."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
)


class BrowserService:
    """Manages a Playwright browser instance and page pool.

    Encapsulates all Playwright lifecycle (launch, page management, teardown)
    so that scrapers interact with a clean, high-level API.

    Args:
        use_vpn: Whether to load a browser extension for VPN support.
        vpn_extension_path: Filesystem path to the unpacked VPN extension.
        multiple_pages: If True, pre-create *max_workers* pages in a pool.
        max_workers: Number of pages to open when *multiple_pages* is True.
        verbose: Enable verbose logging.
        owner_class_name: Used to name the Chromium user-data temp dir.
    """

    def __init__(
        self,
        use_vpn: bool = False,
        vpn_extension_path: Optional[str] = None,
        multiple_pages: bool = False,
        max_workers: int = 50,
        headless: bool = True,
        verbose: bool = False,
        owner_class_name: str = "browser",
    ) -> None:
        self.use_vpn = use_vpn
        self.vpn_extension_path = vpn_extension_path
        self.multiple_pages = multiple_pages
        self.max_workers = max_workers
        self.headless = headless
        self.verbose = verbose
        self.owner_class_name = owner_class_name

        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._browser_context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.pages: list[Page] = []
        self._page_pool: asyncio.Queue[Page] = asyncio.Queue()

    async def initialize(self) -> None:
        """Launch Playwright and open the browser / page pool."""
        self._playwright = await async_playwright().start()

        launch_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ]

        if self.use_vpn and self.vpn_extension_path:
            extension_abs_path = Path(self.vpn_extension_path).resolve().as_posix()
            launch_args += [
                f"--load-extension={extension_abs_path}",
                "--disable-extensions-file-access-check",
                "--allow-running-insecure-content",
                "--disable-web-security",
                "--allow-file-access-from-files",
            ]
            if self.verbose:
                logger.info(
                    f"Attempting to load packed extension from: {extension_abs_path}"
                )
            user_data_dir = f"/tmp/pw-{self.owner_class_name.lower()}"
            self._browser_context = (
                await self._playwright.chromium.launch_persistent_context(
                    user_data_dir,
                    channel="chrome",
                    headless=self.headless,
                    args=launch_args,
                )
            )
            if self.multiple_pages:
                for _ in range(self.max_workers):
                    page = await self._browser_context.new_page()
                    self.pages.append(page)
                    self._page_pool.put_nowait(page)
                    if self.verbose:
                        logger.info(f"Page {len(self.pages) - 1} initialized")
            else:
                self.page = (
                    self._browser_context.pages[0]
                    if self._browser_context.pages
                    else await self._browser_context.new_page()
                )
        else:
            self._browser = await self._playwright.chromium.launch(
                headless=self.headless, args=launch_args
            )
            if self.multiple_pages:
                for page_id in range(self.max_workers):
                    page = await self._browser.new_page()
                    self.pages.append(page)
                    self._page_pool.put_nowait(page)
                    if self.verbose:
                        logger.info(f"Page {page_id} initialized")
            else:
                self.page = await self._browser.new_page()

    async def get_soup(self, url: str, page: Optional[Page] = None) -> BeautifulSoup:
        """Navigate to *url* and return parsed HTML as BeautifulSoup."""
        target_page = page or self.page
        if target_page is None:
            raise RuntimeError("Playwright page is not initialized.")
        await target_page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(1)
        content = await target_page.content()
        return BeautifulSoup(content, "html.parser")

    async def get_available_page(self) -> Page:
        """Return the next available page from the pool (blocks until one is free)."""
        return await self._page_pool.get()

    def release_page(self, page: Page) -> None:
        """Return *page* to the pool for reuse."""
        self._page_pool.put_nowait(page)

    async def cleanup(self) -> None:
        """Close all pages, the browser, and stop Playwright."""
        for p in self.pages:
            try:
                await p.close()
            except Exception:
                pass
        self.pages.clear()
        if self.page:
            try:
                await self.page.close()
            except Exception:
                pass
            self.page = None
        if self._browser_context:
            try:
                await self._browser_context.close()
            except Exception:
                pass
            self._browser_context = None
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None
