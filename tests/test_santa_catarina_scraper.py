"""Tests for SantaCatarinaScraper.

Covers:
- TYPES constant: 2 types (Legislativo/Executivo), values are 'legislativo'/'executivo'
- SITUATIONS constant: empty list
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set
- _format_search_url: correct URL construction
- _get_docs_links:
  - failed soup → stops iteration and returns []
  - no ato-normativo links → empty list
  - valid links → correct doc dicts
  - deduplication via seen_hrefs
  - ementa extracted from card div
- _get_doc_data:
  - missing document_url → None
  - already scraped → None
  - failed soup → None + _save_doc_error
  - invalid markdown (_valid_markdown) → None + _save_doc_error
  - valid markdown → correct dict with inferred norm_type
- norm type inference from title (_RE_TYPE_FROM_TITLE):
  - "LEI" prefix → "Lei"
  - "DECRETO" prefix → "Decreto"
  - "DEC-123" abbreviation → "Decreto"
  - unknown prefix → "Legislação" fallback

Run with:
    .venv/bin/pytest tests/test_santa_catarina_scraper.py -v
"""

from unittest.mock import AsyncMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.santa_catarina import (
    SITUATIONS,
    TYPES,
    SantaCatarinaScraper,
)
from base_tests import TypesConstantTests, ScraperClassTests, SituationsConstantTests
from conftest import make_base_scraper, make_failed_request


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> SantaCatarinaScraper:
    """Instantiate SantaCatarinaScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        SantaCatarinaScraper,
        "https://leis.alesc.sc.gov.br",
        "SANTA_CATARINA",
        TYPES,
        situations=SITUATIONS,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 2
    REQUIRED_KEYS = {"Legislativo", "Executivo"}
    REQUIRE_INT_VALUES = False


# ---------------------------------------------------------------------------
# SITUATIONS constant
# ---------------------------------------------------------------------------


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = list
    EXPECTED_EMPTY = True


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = SantaCatarinaScraper
    STATE_NAME = "Santa Catarina"


# ---------------------------------------------------------------------------
# _format_search_url
# ---------------------------------------------------------------------------


class TestFormatSearchUrl:
    def test_includes_path(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("legislativo", 2023)
        assert "legislativo" in url

    def test_includes_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("legislativo", 2023)
        assert "ano=2023" in url

    def test_starts_with_base_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("executivo", 2020)
        assert url.startswith("https://leis.alesc.sc.gov.br")

    def test_executivo_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("executivo", 2022)
        assert url == "https://leis.alesc.sc.gov.br/executivo?ano=2022"


# ---------------------------------------------------------------------------
# HTML helpers for _get_docs_links
# ---------------------------------------------------------------------------


def _make_link_soup(links_html: str) -> BeautifulSoup:
    html = f"<html><body>{links_html}</body></html>"
    return BeautifulSoup(html, "html.parser")


def _make_card_html(
    href: str = "/ato-normativo/legislativo/123",
    title: str = "Lei 1/2023",
    ementa: str = "Dispõe sobre algo.",
) -> str:
    return (
        f'<div class="card-item">'
        f'<a href="{href}">{title}</a>'
        f'<div class="ementa">{ementa}</div>'
        f"</div>"
    )


# ---------------------------------------------------------------------------
# _get_docs_links
# ---------------------------------------------------------------------------


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_failed_soup_returns_empty(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        result = await scraper._get_docs_links("legislativo", 2023)
        assert result == []

    @pytest.mark.asyncio
    async def test_no_links_returns_empty(self):
        scraper = _make_scraper()
        soup_empty = BeautifulSoup(
            "<html><body><p>Sem links</p></body></html>", "html.parser"
        )
        scraper.request_service.get_soup = AsyncMock(return_value=soup_empty)
        result = await scraper._get_docs_links("legislativo", 2023)
        assert result == []

    @pytest.mark.asyncio
    async def test_valid_link_returns_doc(self):
        scraper = _make_scraper()
        card = _make_card_html(
            href="/ato-normativo/legislativo/42",
            title="Lei 42/2023",
            ementa="Dispõe sobre algo.",
        )
        soup = _make_link_soup(card)
        # First call returns soup with links, subsequent calls return empty soup
        empty_soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[soup, empty_soup, empty_soup, empty_soup]
        )
        result = await scraper._get_docs_links("legislativo", 2023)
        assert len(result) >= 1
        doc = result[0]
        assert doc["title"] == "Lei 42/2023"
        assert "document_url" in doc
        assert "https://leis.alesc.sc.gov.br" in doc["document_url"]

    @pytest.mark.asyncio
    async def test_deduplication_of_hrefs(self):
        """Same href on two consecutive pages should only be added once."""
        scraper = _make_scraper()
        card = _make_card_html(
            href="/ato-normativo/legislativo/99", title="Lei 99/2023"
        )
        soup = _make_link_soup(card)
        # Same soup returned twice → second page should be deduplicated
        empty_soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[soup, soup, empty_soup, empty_soup, empty_soup]
        )
        result = await scraper._get_docs_links("legislativo", 2023)
        hrefs = [doc["document_url"] for doc in result]
        assert len(hrefs) == len(set(hrefs))

    @pytest.mark.asyncio
    async def test_link_without_title_skipped(self):
        scraper = _make_scraper()
        # Link with empty text
        html = '<html><body><a href="/ato-normativo/legislativo/1"></a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        empty_soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[soup, empty_soup, empty_soup, empty_soup]
        )
        result = await scraper._get_docs_links("legislativo", 2023)
        assert result == []


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    def _make_doc(self, **kwargs):
        base = {
            "document_url": "https://leis.alesc.sc.gov.br/ato-normativo/legislativo/42",
            "title": "Lei 42/2023",
            "year": 2023,
            "summary": "Dispõe sobre algo.",
        }
        base.update(kwargs)
        return base

    @pytest.mark.asyncio
    async def test_missing_document_url_returns_none(self):
        scraper = _make_scraper()
        result = await scraper._get_doc_data(
            {"title": "Lei 1/2023", "document_url": ""}
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_already_scraped_returns_none(self):
        scraper = _make_scraper()
        url = "https://leis.alesc.sc.gov.br/ato-normativo/legislativo/42"
        scraper._scraped_keys = {(url, "Lei 42/2023")}
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_failed_soup_returns_none_and_saves_error(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_returns_none_and_saves_error(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>Texto</p></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_whitespace_markdown_returns_none(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>Texto</p></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        scraper._get_markdown = AsyncMock(return_value="   \n\n   ")
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_valid_markdown_returns_doc_with_fields(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>Texto</p></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        valid_md = "# Lei 42/2023\n\n" + "Texto da lei. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        doc = self._make_doc(title="Lei 42/2023")
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["text_markdown"] == valid_md
        assert result["situation"] == "Não consta"
        assert result["_content_extension"] == ".html"
        assert isinstance(result["_raw_content"], bytes)

    @pytest.mark.asyncio
    async def test_norm_type_inferred_from_lei_prefix(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>Texto</p></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        valid_md = "# Lei\n\n" + "Texto. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = self._make_doc(title="LEI 42/2023")
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["type"] == "Lei"

    @pytest.mark.asyncio
    async def test_norm_type_inferred_from_decreto_prefix(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>Texto</p></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        valid_md = "# Decreto\n\n" + "Texto. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = self._make_doc(title="DECRETO 10/2023")
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["type"] == "Decreto"

    @pytest.mark.asyncio
    async def test_norm_type_abbreviation_dec_expanded(self):
        """DEC- abbreviation should map to 'Decreto'."""
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>Texto</p></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        valid_md = "# Decreto\n\n" + "Texto. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = self._make_doc(title="DEC-123 de 2023")
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["type"] == "Decreto"

    @pytest.mark.asyncio
    async def test_norm_type_fallback_to_legislacao(self):
        """Titles with no recognizable prefix fall back to 'Legislação'."""
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>Texto</p></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        valid_md = "# Norma\n\n" + "Texto. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = self._make_doc(title="Norma Especial 5/2023")
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["type"] == "Legislação"
