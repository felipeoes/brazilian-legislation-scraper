import asyncio
from unittest.mock import AsyncMock

from test_store import document

from src.core.http import HttpClient, HttpError
from src.core.scraper import BaseScraper
from src.core.store import Store


class Scraper(BaseScraper):
    name = "TEST"
    base_url = "https://example.org"
    active = 0
    peak = 0

    async def list_documents(self, year):
        if year == 2023:
            raise HttpError(self.base_url, 500, "listing failed")
        for i in range(30):
            yield {"year": year, "document_url": f"listing/{i}", "title": str(i)}

    async def fetch(self, ref):
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(0)
        self.active -= 1
        return document(title=ref["title"], year=1989)


async def test_bounded_workers_year_isolation_and_global_resume():
    async with Store(":memory:") as store:
        scraper = Scraper(
            store=store, http=HttpClient(), years=range(2023, 2025), workers=3
        )
        result = await scraper.run()
        assert result.docs == 30 and result.errors == 1
        assert scraper.peak <= 3
        assert len(await store.keys(1989)) == 30
        scraper.fetch = AsyncMock(side_effect=AssertionError("already saved"))
        result = await scraper.run()
        assert result.docs == 0 and result.skipped == 30 and result.errors == 1
        scraper.fetch.assert_not_called()


async def test_cleanup_attempts_every_resource(tmp_path, monkeypatch):
    monkeypatch.setattr("src.core.scraper.LOG_DIR", tmp_path)
    http = HttpClient()
    http.close = AsyncMock(side_effect=RuntimeError("close failed"))
    browser = AsyncMock()
    store = Store(":memory:")
    scraper = Scraper(store=store, http=http, browser=browser, years=range(2024, 2025))
    import pytest

    with pytest.raises(RuntimeError, match="close failed"):
        async with scraper:
            pass
    browser.close.assert_awaited_once()
    assert store._connection is None


async def test_failed_page_cancels_other_requests_before_next_year():
    import pytest

    scraper = Scraper(
        store=Store(":memory:"), http=HttpClient(), years=range(2024, 2025), workers=2
    )
    waiting = asyncio.Event()
    cancelled = asyncio.Event()

    async def page(number):
        if number == 1:
            await waiting.wait()
            raise HttpError("page", 503, "failed")
        waiting.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    with pytest.raises(HttpError):
        [ref async for ref in scraper.paginate(page, 2)]
    assert cancelled.is_set()
