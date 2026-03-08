"""Tests for RondoniaCotelScraper.

Covers:
- TYPES constant: 4 types with string IDs
- SITUATIONS constant: exists and is an empty list
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set (situations=[] / empty)
- _format_search_url: correct URL construction
- _get_docs_links:
  - failed request → []
  - no table → []
  - empty table → []
  - row without title/pdf link → skipped
  - "NÃO UTILIZADO" summary → skipped
  - relative pdf href → prefixed with base_url
  - valid row → doc dict with correct fields
- _get_doc_data:
  - already scraped → None
  - _download_and_convert returns invalid markdown → None + _save_doc_error called
  - _download_and_convert succeeds → merged dict returned

Run with:
    .venv/bin/pytest tests/test_rondonia_scraper.py -v
"""

from unittest.mock import AsyncMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.rondonia import (
    SITUATIONS,
    TYPES,
    RondoniaCotelScraper,
)
from base_tests import TypesConstantTests, ScraperClassTests, SituationsConstantTests
from conftest import make_base_scraper, make_failed_request


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> RondoniaCotelScraper:
    """Instantiate RondoniaCotelScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        RondoniaCotelScraper,
        "http://ditel.casacivil.ro.gov.br/COTEL",
        "RONDONIA",
        TYPES,
        situations=SITUATIONS,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 4
    REQUIRED_KEYS = {
        "Decreto-Lei",
        "Lei Complementar",
        "Lei Ordinária",
        "Decreto Numerado",
    }
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
    SCRAPER_CLS = RondoniaCotelScraper
    STATE_NAME = "Rondônia"


# ---------------------------------------------------------------------------
# _format_search_url
# ---------------------------------------------------------------------------


class TestFormatSearchUrl:
    def test_includes_type_id(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("leiord", 2023)
        assert "leiord" in url

    def test_includes_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("leiord", 2023)
        assert "2023" in url

    def test_includes_base_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("declei", 2020)
        assert url.startswith("http://ditel.casacivil.ro.gov.br/COTEL")

    def test_correct_pattern(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("leicomp", 2022)
        assert (
            url
            == "http://ditel.casacivil.ro.gov.br/COTEL/Livros/listleicomp.aspx?ano=2022"
        )


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------


def _make_list_soup(rows_html: str) -> BeautifulSoup:
    """Wrap rows in the expected table structure."""
    html = (
        "<html><body>"
        '<table id="ContentPlaceHolder1_DataList1">'
        f"<tbody>{rows_html}</tbody>"
        "</table></body></html>"
    )
    return BeautifulSoup(html, "html.parser")


def _make_valid_row(
    title: str = "Lei Ordinária 1/2023",
    doc_id: str = "42",
    summary: str = "Dispõe sobre algo.",
    pdf_filename: str = "leiord1.pdf",
    detail_href: str = "/COTEL/Livros/detalhes.aspx?id=42",
) -> str:
    return (
        f"<tr><td>"
        f"<div>"
        f'<a href="{detail_href}">{title}</a>'
        f'<a href="/COTEL/Livros/Files/{pdf_filename}">{pdf_filename}</a>'
        f'<span id="ementadocLabel">{summary}</span>'
        f'<span id="coddocLabel">{doc_id}</span>'
        f"</div>"
        f"</td></tr>"
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
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        assert result == []

    @pytest.mark.asyncio
    async def test_no_table_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            "<html><body><p>Sem tabela</p></body></html>", "html.parser"
        )
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        assert result == []

    @pytest.mark.asyncio
    async def test_empty_table_returns_empty(self):
        scraper = _make_scraper()
        soup = _make_list_soup("")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        assert result == []

    @pytest.mark.asyncio
    async def test_row_without_title_link_skipped(self):
        scraper = _make_scraper()
        # Row has a div but no detalhes.aspx link
        row = (
            "<tr><td><div>"
            '<a href="/COTEL/Livros/Files/leiord1.pdf">leiord1.pdf</a>'
            "</div></td></tr>"
        )
        soup = _make_list_soup(row)
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        assert result == []

    @pytest.mark.asyncio
    async def test_row_without_pdf_link_skipped(self):
        scraper = _make_scraper()
        # Row has a detalhes link but no .pdf link
        row = (
            "<tr><td><div>"
            '<a href="/COTEL/Livros/detalhes.aspx?id=1">Lei 1/2023</a>'
            "</div></td></tr>"
        )
        soup = _make_list_soup(row)
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        assert result == []

    @pytest.mark.asyncio
    async def test_nao_utilizado_summary_skipped(self):
        scraper = _make_scraper()
        row = _make_valid_row(summary="NÃO UTILIZADO")
        soup = _make_list_soup(row)
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        assert result == []

    @pytest.mark.asyncio
    async def test_nao_utilizado_case_insensitive(self):
        scraper = _make_scraper()
        row = _make_valid_row(summary="não utilizado")
        soup = _make_list_soup(row)
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        assert result == []

    @pytest.mark.asyncio
    async def test_valid_row_returns_doc(self):
        scraper = _make_scraper()
        row = _make_valid_row()
        soup = _make_list_soup(row)
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_doc_has_expected_fields(self):
        scraper = _make_scraper()
        row = _make_valid_row(
            title="Lei Ordinária 1/2023",
            doc_id="42",
            summary="Dispõe sobre algo.",
            pdf_filename="leiord1.pdf",
        )
        soup = _make_list_soup(row)
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        doc = result[0]
        assert doc["title"] == "Lei Ordinária 1/2023"
        assert doc["id"] == "42"
        assert doc["summary"] == "Dispõe sobre algo."
        assert doc["situation"] == "Não consta"
        assert "pdf_link" in doc

    @pytest.mark.asyncio
    async def test_pdf_link_constructed_from_base_url(self):
        scraper = _make_scraper()
        row = _make_valid_row(pdf_filename="leiord1.pdf")
        soup = _make_list_soup(row)
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        doc = result[0]
        assert (
            doc["pdf_link"]
            == "http://ditel.casacivil.ro.gov.br/COTEL/Livros/Files/leiord1.pdf"
        )

    @pytest.mark.asyncio
    async def test_multiple_valid_rows(self):
        scraper = _make_scraper()
        rows = _make_valid_row(
            title="Lei 1/2023",
            doc_id="1",
            summary="Ementa 1.",
            pdf_filename="lei1.pdf",
            detail_href="/COTEL/Livros/detalhes.aspx?id=1",
        ) + _make_valid_row(
            title="Lei 2/2023",
            doc_id="2",
            summary="Ementa 2.",
            pdf_filename="lei2.pdf",
            detail_href="/COTEL/Livros/detalhes.aspx?id=2",
        )
        soup = _make_list_soup(rows)
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("http://example.com/list.aspx?ano=2023")
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    def _make_doc(self, **kwargs):
        base = {
            "pdf_link": "http://ditel.casacivil.ro.gov.br/COTEL/Livros/Files/lei1.pdf",
            "title": "Lei Ordinária 1/2023",
            "year": 2023,
            "id": "1",
            "summary": "Ementa da lei.",
            "situation": "Não consta",
        }
        base.update(kwargs)
        return base

    @pytest.mark.asyncio
    async def test_already_scraped_returns_none(self):
        scraper = _make_scraper()
        pdf_link = "http://ditel.casacivil.ro.gov.br/COTEL/Livros/Files/lei1.pdf"
        scraper._scraped_keys = {(pdf_link, "Lei Ordinária 1/2023")}
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_invalid_markdown_returns_none_and_saves_error(self):
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(return_value=("short", b"", ".pdf"))
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_markdown_returns_none_and_saves_error(self):
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(return_value=("", b"", ".pdf"))
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_doc_returns_merged_dict(self):
        scraper = _make_scraper()
        pdf_link = "http://ditel.casacivil.ro.gov.br/COTEL/Livros/Files/lei1.pdf"
        pdf_bytes = b"%PDF content"
        valid_md = "# Lei\n\n" + "Texto. " * 30
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, pdf_bytes, ".pdf")
        )
        doc = self._make_doc()
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["text_markdown"] == valid_md
        assert result["document_url"] == pdf_link
        assert result["_raw_content"] == pdf_bytes
        assert result["_content_extension"] == ".pdf"
        assert result["title"] == "Lei Ordinária 1/2023"
        assert result["year"] == 2023
        assert result["summary"] == "Ementa da lei."

    @pytest.mark.asyncio
    async def test_pdf_link_removed_from_result(self):
        scraper = _make_scraper()
        valid_md = "# Lei\n\n" + "Texto. " * 30
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"%PDF content", ".pdf")
        )
        doc = self._make_doc()
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert "pdf_link" not in result

    @pytest.mark.asyncio
    async def test_download_and_convert_called_with_pdf_link(self):
        scraper = _make_scraper()
        pdf_link = "http://ditel.casacivil.ro.gov.br/COTEL/Livros/Files/lei1.pdf"
        valid_md = "# Lei\n\n" + "Texto. " * 30
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"%PDF content", ".pdf")
        )
        await scraper._get_doc_data(self._make_doc())
        scraper._download_and_convert.assert_called_once_with(pdf_link)
