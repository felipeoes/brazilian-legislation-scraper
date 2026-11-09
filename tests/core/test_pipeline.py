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


class TypedScraper(BaseScraper):
    name = "TYPED"
    base_url = "https://example.org"
    types = {"Lei": "lei", "Decreto": "decreto"}

    async def list_type(self, year, name, value):
        if name == "Lei":
            raise HttpError(self.base_url, 500, "type failed")
        yield {"year": year, "type": name, "document_url": value, "title": name}

    async def fetch(self, ref):
        return document(title=ref["title"], year=ref["year"])


async def test_failed_type_does_not_stop_later_types():
    async with Store(":memory:") as store:
        scraper = TypedScraper(store=store, http=HttpClient(), years=range(2024, 2025))
        result = await scraper.run()
        assert result.docs == 1 and result.errors == 1
        assert (
            store.connection.execute("SELECT type FROM errors").fetchone()[0] == "Lei"
        )


async def test_failed_page_is_recorded_and_later_pages_still_listed():
    async with Store(":memory:") as store:
        scraper = Scraper(
            store=store, http=HttpClient(), years=range(2024, 2025), workers=2
        )

        async def page(number):
            if number == 1:
                raise HttpError("page", 503, "failed")
            return [{"document_url": f"page/{number}", "title": str(number)}]

        refs = [ref async for ref in scraper.paginate(page, 3)]
        assert [ref["title"] for ref in refs] == ["2", "3"]
        assert (
            store.connection.execute("SELECT count(*) FROM errors").fetchone()[0] == 1
        )


async def test_overwrite_rescrapes_without_deleting_rows():
    async with Store(":memory:") as store:
        scraper = Scraper(store=store, http=HttpClient(), years=range(2024, 2025))

        async def fetch(ref):
            return document(title=ref["title"], year=2024)

        scraper.fetch = fetch
        await scraper.run()
        scraper.overwrite = True
        scraper.fetch = AsyncMock(side_effect=RuntimeError("site down"))
        result = await scraper.run()
        assert scraper.fetch.await_count == 30 and result.errors == 30
        assert len(await store.keys()) == 30


async def test_workers_move_to_next_year_while_one_document_is_slow():
    released = asyncio.Event()

    class SlowScraper(Scraper):
        async def list_documents(self, year):
            yield {"year": year, "document_url": f"doc/{year}", "title": str(year)}

        async def fetch(self, ref):
            if ref["year"] == 2023:
                await released.wait()
            released.set()
            return document(title=ref["title"], year=ref["year"])

    async with Store(":memory:") as store:
        scraper = SlowScraper(
            store=store, http=HttpClient(), years=range(2023, 2025), workers=2
        )
        result = await asyncio.wait_for(scraper.run(), 2)
        assert result.docs == 2


async def test_yearless_row_without_year_is_an_error():
    class YearlessScraper(Scraper):
        yearless = True

        async def list_documents(self, year):
            yield {"year": None, "document_url": "doc/none", "title": "no year"}
            yield {"year": 2024, "document_url": "doc/2024", "title": "2024"}

    async with Store(":memory:") as store:
        scraper = YearlessScraper(
            store=store, http=HttpClient(), years=range(2024, 2025)
        )
        result = await scraper.run()
        assert result.docs == 1 and result.errors == 1
