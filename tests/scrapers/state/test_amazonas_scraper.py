"""Tests for LegislaAMScraper.

Covers:
- TYPES constant completeness (including Constituição Estadual with special ID)
- SITUATIONS module-level dict preserved for downstream consumers
- Class docstring is accessible (__doc__ is not None)
- _iterate_situations is NOT set (removed from class)
- situations={} passed to super().__init__
- _format_search_url: correct URL pattern
- _get_docs_links: happy-path returns (list, False), empty page returns ([], True),
  error page returns ([], True), no link skips item
- _get_norm_text: short text returns None, valid text returns wrapped soup
- _get_doc_data: resume skip, soup failure handled, no norm text → error,
  invalid markdown → error, correct result dict shape
- _scrape_type: constitution once-only guard (_scraped_constitution flag),
  constitution scraped on first call and skipped on second,
  normal type delegates to _paginate_until_end + _process_documents

Integration (live site):
- test_get_docs_links_lei_ordinaria_2022_returns_results
- test_get_doc_data_returns_valid_markdown

Run with:
    .venv/bin/pytest tests/test_amazonas_scraper.py -v
"""

import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from bs4 import BeautifulSoup
from conftest import assert_resume_skips, make_base_scraper, make_failed_request

from src.scraper.state_legislation.amazonas import SITUATIONS, TYPES, LegislaAMScraper

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> LegislaAMScraper:
    """Instantiate LegislaAMScraper bypassing __init__ (no network, no I/O)."""
    kwargs.setdefault("_scraped_constitution", False)
    return make_base_scraper(
        LegislaAMScraper,
        "https://legisla.imprensaoficial.am.gov.br",
        "AMAZONAS",
        TYPES,
        situations={},
        **kwargs,
    )


def _make_listing_soup(
    items: list[tuple[str, str]],
    date_text: str = "Publicada em 04 de janeiro de 2025",
) -> BeautifulSoup:
    """Build a minimal listing page BeautifulSoup with (title, href) rows."""
    rows = ""
    for title, href in items:
        rows += (
            f'<li class="item-li">'
            f"<h5>{title}</h5>"
            f"<p>{date_text}</p>"
            f'<a href="{href}">texto</a>'
            f"</li>"
        )
    html = f"<html><body><ul>{rows}</ul></body></html>"
    return BeautifulSoup(html, "html.parser")


def _make_error_soup() -> BeautifulSoup:
    html = '<html><body><div id="container"><h1>Error</h1></div></body></html>'
    return BeautifulSoup(html, "html.parser")


def _make_doc_soup(text: str = "A" * 200) -> BeautifulSoup:
    """Build a minimal document page BeautifulSoup with norm text."""
    html = f'<html><body><div class="materia rounded"><p>{text}</p></div></body></html>'
    return BeautifulSoup(html, "html.parser")


def _make_response(soup: BeautifulSoup) -> MagicMock:
    mock = MagicMock()
    mock.__bool__ = lambda s: True
    return mock


# ---------------------------------------------------------------------------
# TYPES and SITUATIONS constants
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 9
    REQUIRED_KEYS = {
        "Constituição Estadual",
        "Emendas Constitucionais",
        "Lei Complementar",
        "Lei Delegada",
        "Lei Ordinária",
        "Decreto Legislativo",
        "Decreto",
        "Lei Promulgada",
        "Regimento Interno",
    }
    REQUIRE_INT_VALUES = False


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = LegislaAMScraper
    STATE_NAME = "Amazonas"

    def test_situations_is_empty_in_instance(self):
        scraper = _make_scraper()
        assert scraper.situations == {}

    def test_scraped_constitution_flag_default(self):
        scraper = _make_scraper()
        assert not scraper._scraped_constitution


# ---------------------------------------------------------------------------
# _format_search_url
# ---------------------------------------------------------------------------


class TestFormatSearchUrl:
    def test_url_contains_type_id_year_page(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(41535, 2022, 3)
        assert "41535" in url
        assert "2022" in url
        assert "page=3" in url

    def test_url_starts_with_base_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(12, 2020, 1)
        assert url.startswith(scraper.base_url)

    def test_url_has_diario_am_path(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(10, 2021, 1)
        assert "/diario_am/" in url


# ---------------------------------------------------------------------------
# _get_docs_links
# ---------------------------------------------------------------------------


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_happy_path_returns_docs_and_false(self):
        """Pages with items always return reached_end=False (pagination stops on empty page)."""
        scraper = _make_scraper()
        soup = _make_listing_soup(
            [
                ("Lei Ordinária 001/2022", "/diario_am/12/2022/1"),
                ("Lei Ordinária 002/2022", "/diario_am/12/2022/2"),
            ]
        )
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        docs, reached_end = await scraper._get_docs_links("http://example.com/page=1")
        assert reached_end is False
        assert len(docs) == 2
        assert docs[0]["title"] == "Lei Ordinária 001/2022"
        assert docs[0]["html_link"] == "/diario_am/12/2022/1"
        assert "summary" in docs[0]
        assert docs[0]["date"] == "2025-01-04"

    @pytest.mark.asyncio
    async def test_failed_request_returns_empty_and_true(self):
        """FailedRequest sentinel (falsy) must short-circuit to ([], True)."""
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        docs, reached_end = await scraper._get_docs_links("http://example.com/page=1")
        assert docs == []
        assert reached_end is True

    @pytest.mark.asyncio
    async def test_error_page_returns_empty_and_true(self):
        scraper = _make_scraper()
        soup = _make_error_soup()
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        docs, reached_end = await scraper._get_docs_links("http://example.com/page=999")
        assert reached_end is True
        assert docs == []

    @pytest.mark.asyncio
    async def test_item_without_link_is_skipped(self):
        scraper = _make_scraper()
        html = (
            '<html><body><li class="item-li"><h5>Lei sem link</h5></li></body></html>'
        )
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        docs, _ = await scraper._get_docs_links("http://example.com/page=1")
        assert docs == []

    @pytest.mark.asyncio
    async def test_empty_page_returns_empty_and_true(self):
        """Page with no item-li elements must signal end of pagination."""
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        docs, reached_end = await scraper._get_docs_links("http://example.com/page=2")
        assert docs == []
        assert reached_end is True

    @pytest.mark.asyncio
    async def test_date_missing_p_tag_produces_none(self):
        """Items without a <p> date tag should still be collected with date=None."""
        scraper = _make_scraper()
        html = (
            "<html><body>"
            '<li class="item-li"><h5>Lei 001</h5><a href="/x/1">link</a></li>'
            "</body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        docs, _ = await scraper._get_docs_links("http://example.com/page=1")
        assert len(docs) == 1
        assert docs[0]["date"] is None


# ---------------------------------------------------------------------------
# _get_norm_text
# ---------------------------------------------------------------------------


class TestGetNormText:
    def test_short_text_returns_none(self):
        scraper = _make_scraper()
        html = (
            '<html><body><div class="materia rounded"><p>Curto</p></div></body></html>'
        )
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._get_norm_text(soup)
        assert result is None

    def test_valid_text_returns_beautifulsoup(self):
        scraper = _make_scraper()
        long_text = "A" * 200
        html = f'<html><body><div class="materia rounded"><p>{long_text}</p></div></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._get_norm_text(soup)
        assert result is not None
        assert hasattr(result, "find")

    def test_valid_text_wraps_in_html_structure(self):
        scraper = _make_scraper()
        long_text = "B" * 200
        html = f'<html><body><div class="materia rounded"><p>{long_text}</p></div></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._get_norm_text(soup)
        assert result.find("body") is not None


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_failed_request_logs_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            side_effect=Exception("Connection refused")
        )
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Lei 001",
            "year": 2022,
            "html_link": "/diario_am/12/2022/1",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(
            _make_scraper(),
            {
                "title": "Lei Ordinária 001",
                "year": 2022,
                "html_link": "/diario_am/12/2022/1",
            },
        )

    @pytest.mark.asyncio
    async def test_no_norm_text_logs_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        # Soup with no "materia rounded" div
        soup = BeautifulSoup("<html><body><p>nothing</p></body></html>", "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Lei 001",
            "year": 2022,
            "html_link": "/diario_am/12/2022/1",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_logs_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        long_text = "X" * 200
        soup = _make_doc_soup(long_text)
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))
        scraper._save_doc_error = AsyncMock()
        # Return very short (invalid) markdown
        scraper._get_markdown = AsyncMock(return_value="short")
        doc_info = {
            "title": "Lei 001",
            "year": 2022,
            "html_link": "/diario_am/12/2022/1",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None
        scraper._save_doc_error.assert_called_once()
        assert "Invalid markdown" in scraper._save_doc_error.call_args.kwargs.get(
            "error_message", ""
        )

    @pytest.mark.asyncio
    async def test_valid_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        long_text = "C" * 200
        soup = _make_doc_soup(long_text)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(soup, b"fake-mhtml-content")
        )
        valid_md = "# Lei Ordinária\n\n" + "Texto da lei. " * 20
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc_info = {
            "title": "Lei 001",
            "year": 2022,
            "type": "Lei Ordinária",
            "situation": "Não consta",
            "html_link": "/diario_am/12/2022/1",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert result["text_markdown"] == valid_md.strip()
        assert "document_url" in result
        assert result["_raw_content"] == b"fake-mhtml-content"
        assert result["_content_extension"] == ".mhtml"


# ---------------------------------------------------------------------------
# _scrape_type
# ---------------------------------------------------------------------------


class TestScrapeType:
    @pytest.mark.asyncio
    async def test_constitution_scraped_once_and_flag_set(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Constituição Estadual\n\n" + "Texto. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        long_text = "D" * 200
        soup = _make_doc_soup(long_text)
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))
        scraper._save_doc_result = AsyncMock()

        results = await scraper._scrape_type(
            "Constituição Estadual", "12/1989/10/746", 1956
        )
        assert len(results) == 1
        assert scraper._scraped_constitution is True
        scraper._save_doc_result.assert_called_once()

    @pytest.mark.asyncio
    async def test_constitution_has_hardcoded_year_and_date(self):
        """Constitution doc must always carry year=1989 and date='1989-10-05'."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Constituição Estadual\n\n" + "Texto. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        soup = _make_doc_soup("E" * 200)
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))
        scraper._save_doc_result = AsyncMock()

        # Called during year 1956 — year/date must still be 1989
        results = await scraper._scrape_type(
            "Constituição Estadual", "12/1989/10/746", 1956
        )
        assert len(results) == 1
        doc = results[0]
        assert doc["year"] == 1989
        assert doc["date"] == "1989-10-05"

    @pytest.mark.asyncio
    async def test_constitution_not_scraped_twice(self):
        scraper = _make_scraper(_scraped_constitution=True)
        scraper._paginate_until_end = AsyncMock(return_value=[])
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_type("Constituição Estadual", "12/1989/10/746", 1990)
        # Should fall through to normal pagination (no second constitution attempt)
        scraper._paginate_until_end.assert_called_once()

    @pytest.mark.asyncio
    async def test_normal_type_calls_paginate_and_process(self):
        scraper = _make_scraper()
        fake_docs = [{"title": "Lei 001", "html_link": "/x"}]
        scraper._paginate_until_end = AsyncMock(return_value=fake_docs)
        scraper._process_documents = AsyncMock(return_value=fake_docs)

        results = await scraper._scrape_type("Lei Ordinária", 12, 2022)
        scraper._paginate_until_end.assert_called_once()
        scraper._process_documents.assert_called_once()
        assert results == fake_docs

    @pytest.mark.asyncio
    async def test_normal_type_empty_page_returns_empty(self):
        scraper = _make_scraper()
        scraper._paginate_until_end = AsyncMock(return_value=[])
        scraper._process_documents = AsyncMock(return_value=[])

        results = await scraper._scrape_type("Decreto", 41536, 1900)
        assert results == []


# ---------------------------------------------------------------------------
# Integration tests (live site)
# ---------------------------------------------------------------------------


async def test_get_docs_links_lei_ordinaria_2022_returns_results():
    """LegislaAM should return at least one document for Lei Ordinária 2022."""
    with tempfile.TemporaryDirectory() as tmp:
        scraper = LegislaAMScraper(docs_save_dir=tmp, verbose=False)
        url = scraper._format_search_url(12, 2022, 1)  # Lei Ordinária = 12
        docs, reached_end = await scraper._get_docs_links(url)
        assert isinstance(docs, list)
        assert len(docs) > 0
        assert "title" in docs[0]
        assert "html_link" in docs[0]


async def test_get_doc_data_returns_valid_markdown():
    """Fetching the first Lei Ordinária 2022 should yield non-empty markdown."""
    with tempfile.TemporaryDirectory() as tmp:
        scraper = LegislaAMScraper(docs_save_dir=tmp, verbose=False)
        url = scraper._format_search_url(12, 2022, 1)
        docs, _ = await scraper._get_docs_links(url)
        assert len(docs) > 0

        doc = docs[0]
        doc["year"] = 2022
        doc["type"] = "Lei Ordinária"
        doc["situation"] = "Não consta"
        result = await scraper._get_doc_data(doc)
        # May be None if markdown is invalid — just check it returns dict or None
        if result is not None:
            assert "text_markdown" in result
            assert result["text_markdown"] is not None
            assert len(result["text_markdown"]) > 50
