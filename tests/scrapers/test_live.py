"""Live source checks; excluded from the offline suite and pre-commit."""

import pytest

from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION
from src.scrapers.states.am import LegislaAMScraper
from src.scrapers.states.ba import BahiaLegislaScraper

pytestmark = pytest.mark.integration


async def test_amazonas_listing(integration_scraper_factory):
    async with integration_scraper_factory(LegislaAMScraper, year=2022) as scraper:
        docs, _ = await scraper._get_docs_links(scraper._format_search_url(12, 2022, 1))
        assert docs and docs[0]["html_link"]


async def test_amazonas_fetch(integration_scraper_factory):
    async with integration_scraper_factory(LegislaAMScraper, year=2022) as scraper:
        docs, _ = await scraper._get_docs_links(scraper._format_search_url(12, 2022, 1))
        assert docs
        result = await scraper.fetch(
            {
                **docs[0],
                "year": 2022,
                "type": "Lei Ordinária",
                "situation": DEFAULT_VALID_SITUATION,
            }
        )
        assert isinstance(result, Document)


async def test_bahia_listing(integration_scraper_factory):
    async with integration_scraper_factory(BahiaLegislaScraper, year=2024) as scraper:
        refs = [ref async for ref in scraper.list_documents(2024)]
        assert refs and all(ref["type"] for ref in refs)


async def test_bahia_constitution(integration_scraper_factory):
    async with integration_scraper_factory(BahiaLegislaScraper, year=1989) as scraper:
        docs = await scraper._get_docs_links(scraper._build_search_url(11, 1989, 0))
        assert docs
        result = await scraper.fetch(
            {**docs[0], "year": 1989, "situation": DEFAULT_VALID_SITUATION}
        )
        assert isinstance(result, Document) and result.year == 1989
