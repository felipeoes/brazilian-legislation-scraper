"""Tests for RNAlrnScraper (Rio Grande do Norte).

Covers:
- TYPES constant: 5 types present, string IDs
- SITUATIONS constant: exists at module level with 'Não consta'
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set (situations passed but scraper uses default flow)
- _format_search_url: returns correctly encoded URL
- _get_docs_links:
  - failed request → []
  - table missing → [] (null guard)
  - empty table → []
  - valid rows → list of doc dicts
- _get_doc_data:
  - already scraped → None
  - failed request → None
  - invalid markdown (_valid_markdown) → None
  - valid markdown → correct dict shape

Run with:
    .venv/bin/pytest tests/test_rio_grande_do_norte_scraper.py -v
"""

from unittest.mock import AsyncMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.rio_grande_do_norte import (
    SITUATIONS,
    TYPES,
    RNAlrnScraper,
)
from base_tests import TypesConstantTests, ScraperClassTests, SituationsConstantTests
from conftest import make_base_scraper, make_failed_request


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> RNAlrnScraper:
    """Instantiate RNAlrnScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        RNAlrnScraper,
        "https://www.al.rn.leg.br",
        "RIO_GRANDE_DO_NORTE",
        TYPES,
        situations=SITUATIONS,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 5
    REQUIRED_KEYS = {
        "Lei Ordinária",
        "Lei Complementar",
        "Emenda Constitucional",
        "Constituição Estadual",
    }
    REQUIRE_INT_VALUES = False


# ---------------------------------------------------------------------------
# SITUATIONS constant
# ---------------------------------------------------------------------------


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = RNAlrnScraper
    STATE_NAME = "Rio Grande do Norte"


# ---------------------------------------------------------------------------
# _format_search_url
# ---------------------------------------------------------------------------


class TestFormatSearchUrl:
    def test_includes_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2023, 1)
        assert "nome=2023" in url

    def test_includes_page_number(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2023, 3)
        assert "page=3" in url

    def test_base_url_included(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2023, 1)
        assert url.startswith("https://www.al.rn.leg.br")


# ---------------------------------------------------------------------------
# _get_docs_links
# ---------------------------------------------------------------------------


def _make_table_soup(rows_html: str) -> BeautifulSoup:
    html = (
        "<html><body>"
        '<table class="table table-sm table-striped">'
        f"{rows_html}"
        "</table></body></html>"
    )
    return BeautifulSoup(html, "html.parser")


def _make_row(title: str, year: int, pdf_href: str) -> str:
    return (
        f'<tr><th>{title}</th><td>{year}</td><td><a href="{pdf_href}">PDF</a></td></tr>'
    )


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_failed_request_returns_empty(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._get_docs_links("http://example.com/page=1")
        assert result == []

    @pytest.mark.asyncio
    async def test_no_table_returns_empty(self):
        """When the table element is not found, return [] instead of crashing."""
        scraper = _make_scraper()
        soup = BeautifulSoup(
            "<html><body><p>Sem tabela</p></body></html>", "html.parser"
        )
        result = await scraper._get_docs_links("http://example.com", soup=soup)
        assert result == []

    @pytest.mark.asyncio
    async def test_empty_table_returns_empty(self):
        scraper = _make_scraper()
        soup = _make_table_soup("")
        result = await scraper._get_docs_links("http://example.com", soup=soup)
        assert result == []

    @pytest.mark.asyncio
    async def test_header_row_skipped(self):
        """Rows without <td> elements (e.g. header <th> only) are skipped."""
        scraper = _make_scraper()
        soup = _make_table_soup("<tr><th>Número</th><th>Ano</th><th>PDF</th></tr>")
        result = await scraper._get_docs_links("http://example.com", soup=soup)
        assert result == []

    @pytest.mark.asyncio
    async def test_valid_row_returns_doc(self):
        scraper = _make_scraper()
        row = _make_row("Lei Ordinária 42/2022", 2022, "/pdf/lei42.pdf")
        soup = _make_table_soup(row)
        result = await scraper._get_docs_links("http://example.com", soup=soup)
        assert len(result) == 1
        doc = result[0]
        assert doc["title"] == "Lei Ordinária 42/2022"
        assert doc["year"] == 2022
        assert doc["pdf_link"] == "/pdf/lei42.pdf"
        assert "summary" in doc

    @pytest.mark.asyncio
    async def test_multiple_rows_returned(self):
        scraper = _make_scraper()
        rows = _make_row("Lei 1/2020", 2020, "/pdf/lei1.pdf") + _make_row(
            "Lei 2/2021", 2021, "/pdf/lei2.pdf"
        )
        soup = _make_table_soup(rows)
        result = await scraper._get_docs_links("http://example.com", soup=soup)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    def _make_doc(self, **kwargs):
        base = {
            "pdf_link": "https://www.al.rn.leg.br/pdf/lei42.pdf",
            "title": "Lei Ordinária 42/2022",
            "year": 2022,
            "summary": "",
        }
        base.update(kwargs)
        return base

    @pytest.mark.asyncio
    async def test_already_scraped_returns_none(self):
        scraper = _make_scraper()
        scraper._scraped_keys = {
            ("https://www.al.rn.leg.br/pdf/lei42.pdf", "Lei Ordinária 42/2022")
        }
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_failed_request_returns_none(self):
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(return_value=("", b"", ""))
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(return_value=("short", b"", ".pdf"))
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_whitespace_only_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(
            return_value=("   \n\n   ", b"", ".pdf")
        )
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_valid_markdown_returns_correct_shape(self):
        scraper = _make_scraper()
        pdf_bytes = b"%PDF-1.4 content"
        valid_md = "# Lei Ordinária\n\n" + "Texto da lei. " * 30
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, pdf_bytes, ".pdf")
        )

        doc = self._make_doc()
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["text_markdown"] == valid_md
        assert result["document_url"] == "https://www.al.rn.leg.br/pdf/lei42.pdf"
        assert result["_raw_content"] == pdf_bytes
        assert result["_content_extension"] == ".pdf"

    @pytest.mark.asyncio
    async def test_download_and_convert_called_with_pdf_link(self):
        scraper = _make_scraper()
        pdf_link = "https://www.al.rn.leg.br/pdf/lei42.pdf"
        valid_md = "# Lei\n\n" + "Texto. " * 30
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"%PDF content", ".pdf")
        )

        await scraper._get_doc_data(self._make_doc())
        scraper._download_and_convert.assert_called_once_with(pdf_link)
