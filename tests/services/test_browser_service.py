"""Unit tests for BrowserService page pool and overflow behaviour."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.browser.playwright import BrowserService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_service(max_workers: int = 2) -> BrowserService:
    """Return an uninitialised BrowserService with mocked internals."""
    svc = BrowserService(multiple_pages=True, max_workers=max_workers)

    # Fake browser context whose new_page() returns a fresh mock Page
    ctx = MagicMock()
    ctx.new_page = AsyncMock(side_effect=lambda: _mock_page())
    svc._browser_context = ctx
    return svc


def _mock_page(closed: bool = False) -> MagicMock:
    page = MagicMock()
    page.is_closed.return_value = closed
    page.close = AsyncMock()
    return page


# ---------------------------------------------------------------------------
# Normal acquire / release (pool stays intact)
# ---------------------------------------------------------------------------


async def test_get_available_page_returns_pool_page():
    """get_available_page pops from the pool when a page is available."""
    svc = _make_service()
    p = _mock_page()
    svc._page_pool.put_nowait(p)

    result = await svc.get_available_page()

    assert result is p
    assert svc._page_pool.empty()


async def test_release_page_returns_page_to_pool():
    """release_page for a normal page puts it back on the queue."""
    svc = _make_service()
    p = _mock_page()
    svc._page_pool.put_nowait(p)

    acquired = await svc.get_available_page()
    assert svc._page_pool.empty()

    svc.release_page(acquired)
    assert svc._page_pool.qsize() == 1


async def test_closed_pool_page_is_replaced():
    """A closed page from the pool is transparently replaced by a new one."""
    svc = _make_service()
    closed_page = _mock_page(closed=True)
    svc._page_pool.put_nowait(closed_page)

    new_page = _mock_page()
    svc._browser_context.new_page = AsyncMock(return_value=new_page)

    result = await svc.get_available_page()

    assert result is new_page
    svc._browser_context.new_page.assert_awaited_once()


# ---------------------------------------------------------------------------
# Overflow path (pool exhausted → temporary page)
# ---------------------------------------------------------------------------


async def test_overflow_creates_temp_page_on_timeout():
    """When the pool is empty for 180 s a temporary page is created."""
    svc = _make_service()
    # Pool is empty — asyncio.wait_for will time out immediately in tests
    temp_page = _mock_page()
    svc._browser_context.new_page = AsyncMock(return_value=temp_page)

    with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
        result = await svc.get_available_page()

    assert result is temp_page
    assert temp_page in svc._temp_pages


async def test_overflow_logs_warning():
    """A warning is emitted when a temp page is spawned."""
    svc = _make_service()
    svc._browser_context.new_page = AsyncMock(return_value=_mock_page())

    with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
        with patch("src.services.browser.playwright.logger") as mock_logger:
            await svc.get_available_page()

    mock_logger.warning.assert_called_once()
    msg = mock_logger.warning.call_args[0][0].lower()
    assert "exhausted" in msg or "overflow" in msg


async def test_release_temp_page_closes_it_not_requeues():
    """Releasing a temp page closes it and does NOT put it back in the pool."""
    svc = _make_service()
    temp_page = _mock_page()
    svc._browser_context.new_page = AsyncMock(return_value=temp_page)

    with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
        acquired = await svc.get_available_page()

    assert acquired in svc._temp_pages

    # create_task needs a running loop; use a real task
    svc.release_page(acquired)
    await asyncio.sleep(0)  # let the created task run

    acquired.close.assert_awaited_once()
    assert acquired not in svc._temp_pages
    assert svc._page_pool.empty()


async def test_release_normal_page_not_closed():
    """Releasing a regular pool page does NOT close it."""
    svc = _make_service()
    p = _mock_page()
    svc._page_pool.put_nowait(p)

    acquired = await svc.get_available_page()
    svc.release_page(acquired)

    p.close.assert_not_awaited()
    assert svc._page_pool.qsize() == 1


# ---------------------------------------------------------------------------
# Cleanup drains temp pages
# ---------------------------------------------------------------------------


async def test_cleanup_closes_unreleased_temp_pages():
    """cleanup() closes any temp pages that were never released."""
    svc = _make_service()
    temp1 = _mock_page()
    temp2 = _mock_page()
    svc._temp_pages.update({temp1, temp2})

    # Stub out the rest of cleanup so we don't need a real browser
    svc.pages = []
    svc.page = None
    svc._browser_context = None
    svc._browser = None
    svc._playwright = None

    await svc.cleanup()

    temp1.close.assert_awaited_once()
    temp2.close.assert_awaited_once()
    assert len(svc._temp_pages) == 0
