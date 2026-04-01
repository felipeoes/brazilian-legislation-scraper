from __future__ import annotations

from typing import TYPE_CHECKING

from bs4 import BeautifulSoup
from playwright.async_api import Page

from src.services.browser.playwright import BrowserService

if TYPE_CHECKING:
    pass


class BrowserMixin:
    """Mixin that groups all Playwright / MHTML browser helpers.

    Expects the host class to define:
    * ``browser_service``   – ``BrowserService | None``
    * ``_mhtml_browser``    – ``BrowserService | None``
    * ``max_workers``       – ``int``
    * ``request_service``   – ``RequestService`` (used for retry count & rate limiter)
    """

    # ------------------------------------------------------------------
    # Playwright (async browser automation)
    # ------------------------------------------------------------------

    @property
    def page(self) -> Page | None:
        """Active Playwright page (single-page mode)."""
        return self.browser_service.page if self.browser_service else None

    async def initialize_playwright(self):
        """Initialize Playwright browser (async — must be called from scrape())."""
        if self.browser_service:
            await self.browser_service.initialize()

    async def _get_available_page(self) -> Page:
        """Get available page from the pool (async)."""
        if not self.browser_service:
            raise RuntimeError("Browser service is not initialized.")
        return await self.browser_service.get_available_page()

    async def _release_page(self, page: Page):
        """Release page back to the pool."""
        if self.browser_service:
            self.browser_service.release_page(page)

    # ------------------------------------------------------------------
    # MHTML capture
    # ------------------------------------------------------------------

    _MHTML_ERROR_MARKERS = (
        b"Azion - Default error page",
        b"<title>403 Forbidden</title>",
        b"<title>Access Denied</title>",
        b"<title>Error</title>",
        b"Acesso Proibido",
        b"Erro 403",
        b"error/error.css",
        b"The website encountered an unexpected error",
    )

    @classmethod
    def _is_mhtml_error_page(cls, content: bytes) -> bool:
        head = content[:8192]
        return any(marker in head for marker in cls._MHTML_ERROR_MARKERS)

    async def _capture_mhtml(self, url: str) -> bytes:
        """Capture an MHTML snapshot of a URL using a lazily-initialized browser.

        Retries up to 3 times with configurable ``_mhtml_timeout`` per attempt.
        Raises RuntimeError if the captured content looks like an error page.
        """
        if self._mhtml_browser is None:
            self._mhtml_browser = BrowserService(
                headless=True,
                multiple_pages=True,
                max_workers=self.max_workers,
                owner_class_name=f"{self.__class__.__name__}_mhtml",
            )
            await self._mhtml_browser.initialize()

        page = await self._mhtml_browser.get_available_page()
        try:
            last_error: Exception | None = None
            for _ in range(self.request_service.max_retries):
                try:
                    result = await self._mhtml_browser.capture_mhtml(
                        url,
                        page=page,
                        wait_until=self._mhtml_wait_until,
                        timeout=self._mhtml_timeout,
                    )
                    if self._is_mhtml_error_page(result):
                        raise RuntimeError(
                            f"MHTML capture returned error page for {url}"
                        )
                    return result
                except Exception as e:
                    last_error = e
            raise last_error  # type: ignore[misc]
        finally:
            self._mhtml_browser.release_page(page)

    async def _fetch_soup_and_mhtml(
        self,
        url: str,
        *,
        wait_until: str | None = None,
        timeout: int | None = None,
        wait_for_selector: str | None = None,
    ) -> tuple[BeautifulSoup, bytes]:
        """Fetch a URL via browser, returning ``(BeautifulSoup, mhtml_bytes)``.

        Single navigation: extracts both rendered HTML (for parsing/markdown)
        and MHTML archive (for raw file storage). Uses the lazily-initialized
        ``_mhtml_browser`` with page pool, retries, and error page detection.

        If *wait_for_selector* is given, waits for that CSS selector to
        appear in the DOM after navigation (useful for SPA pages that
        render content asynchronously via JS).
        """
        if self._mhtml_browser is None:
            self._mhtml_browser = BrowserService(
                headless=True,
                multiple_pages=True,
                max_workers=self.max_workers,
                owner_class_name=f"{self.__class__.__name__}_mhtml",
            )
            await self._mhtml_browser.initialize()

        page = await self._mhtml_browser.get_available_page()
        try:
            last_error: Exception | None = None
            for _ in range(self.request_service.max_retries):
                try:
                    await self.request_service._rate_limiter.acquire()
                    html, mhtml = await self._mhtml_browser.fetch_and_capture(
                        url,
                        page=page,
                        wait_until=wait_until or self._mhtml_wait_until,
                        timeout=timeout or self._mhtml_timeout,
                        wait_for_selector=wait_for_selector,
                    )
                    if self._is_mhtml_error_page(mhtml):
                        raise RuntimeError(f"Browser returned error page for {url}")
                    return BeautifulSoup(html, "html.parser"), mhtml
                except Exception as e:
                    last_error = e
            raise last_error  # type: ignore[misc]
        finally:
            self._mhtml_browser.release_page(page)
