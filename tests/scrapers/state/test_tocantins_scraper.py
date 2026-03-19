"""Tests for TocantinsScraper.

Covers:
- TYPES constant: 2 types with string IDs
- SITUATIONS constant: dict with "Não consta"
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set
- _format_search_payload: correct payload construction
- _extract_docs_from_soup: HTML parsing logic
  - missing h4 → row skipped
  - missing link in h4 → row skipped
  - relative doc_link → prefixed with base_url
  - date extracted from "Data:" small tag
  - summary extracted from em > strong
  - PDF link extracted from Download anchor
  - valid row → correct doc dict
- _extract_total_count:
  - no matching div → None
  - div with "Registros encontrados" + strong → int
- _has_table_artifacts:
  - no pipes → False
  - many pipes → True
  - many pipe-starting lines → True
- _normalize_table_markdown: collapses pipe tables
- _get_doc_data:
  - missing pdf_link → None + _save_doc_error
  - already scraped → None
  - failed request → None + _save_doc_error
  - invalid markdown (_valid_markdown) → None + _save_doc_error
  - table artifacts + valid markdown → normalized text returned
  - valid markdown → correct dict shape, pdf_link removed

Run with:
    .venv/bin/pytest tests/test_tocantins_scraper.py -v
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from bs4 import BeautifulSoup
from conftest import make_base_scraper, make_failed_request

from src.scraper.state_legislation.tocantins import (
    SITUATIONS,
    TYPES,
    TocantinsScraper,
)

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> TocantinsScraper:
    """Instantiate TocantinsScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        TocantinsScraper,
        "https://www.al.to.leg.br",
        "TOCANTINS",
        TYPES,
        situations=SITUATIONS,
        search_url="https://www.al.to.leg.br/legislacaoEstadual",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 2
    REQUIRED_KEYS = {"Lei Ordinária", "Lei Complementar"}
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
    SCRAPER_CLS = TocantinsScraper
    STATE_NAME = "Tocantins"


# ---------------------------------------------------------------------------
# _format_search_payload
# ---------------------------------------------------------------------------


class TestFormatSearchPayload:
    def test_includes_type_id(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload("ordinaria", 2023)
        assert payload["documento.tipo"] == "ordinaria"

    def test_includes_year(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload("ordinaria", 2023)
        assert payload["documento.ano"] == "2023"

    def test_default_page_is_1(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload("ordinaria", 2023)
        assert payload["pagPaginaAtual"] == "1"

    def test_custom_page(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload("complementar", 2022, page=3)
        assert payload["pagPaginaAtual"] == "3"

    def test_required_keys_present(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload("ordinaria", 2023)
        for key in (
            "pagPaginaAtual",
            "documento.ano",
            "documento.tipo",
            "documento.numero",
        ):
            assert key in payload


# ---------------------------------------------------------------------------
# HTML helpers for _extract_docs_from_soup
# ---------------------------------------------------------------------------


def _make_row_html(
    title: str = "Lei Ordinária 1/2023",
    doc_link: str = "/legislacaoEstadual/detalhe?id=1",
    date: str = "01/01/2023",
    summary: str = "Dispõe sobre algo.",
    pdf_href: str = "/arquivos/lei1.pdf",
) -> str:
    return (
        f'<div class="row">'
        f'<h4><a href="{doc_link}">{title}</a></h4>'
        f"<small>Data: {date}</small>"
        f"<em><strong>{summary}</strong></em>"
        f'<a href="{pdf_href}" title="Download">Download</a>'
        f"</div>"
    )


# ---------------------------------------------------------------------------
# _extract_docs_from_soup
# ---------------------------------------------------------------------------


class TestExtractDocsFromSoup:
    def test_empty_html_returns_empty(self):
        scraper = _make_scraper()
        result = scraper._extract_docs_from_soup(b"<html><body></body></html>")
        assert result == []

    def test_row_without_h4_skipped(self):
        scraper = _make_scraper()
        html = b'<html><body><div class="row"><p>No h4</p></div></body></html>'
        result = scraper._extract_docs_from_soup(html)
        assert result == []

    def test_row_without_link_in_h4_skipped(self):
        scraper = _make_scraper()
        html = b'<html><body><div class="row"><h4>No link</h4></div></body></html>'
        result = scraper._extract_docs_from_soup(html)
        assert result == []

    def test_valid_row_returns_doc(self):
        scraper = _make_scraper()
        html = _make_row_html().encode("utf-8")
        result = scraper._extract_docs_from_soup(html)
        assert len(result) == 1

    def test_doc_has_expected_fields(self):
        scraper = _make_scraper()
        html = _make_row_html(
            title="Lei Ordinária 42/2023",
            doc_link="/legislacaoEstadual/detalhe?id=42",
            date="15/06/2023",
            summary="Dispõe sobre algo.",
            pdf_href="/arquivos/lei42.pdf",
        ).encode("utf-8")
        result = scraper._extract_docs_from_soup(html)
        doc = result[0]
        assert doc["title"] == "Lei Ordinária 42/2023"
        assert "15/06/2023" in doc["date"]
        assert doc["summary"] == "Dispõe sobre algo."
        assert doc["situation"] == "Não consta"
        assert "pdf_link" in doc

    def test_relative_pdf_link_prefixed(self):
        scraper = _make_scraper()
        html = _make_row_html(pdf_href="/arquivos/lei1.pdf").encode("utf-8")
        result = scraper._extract_docs_from_soup(html)
        assert result[0]["pdf_link"].startswith("https://www.al.to.leg.br")

    def test_absolute_pdf_link_unchanged(self):
        scraper = _make_scraper()
        html = _make_row_html(pdf_href="https://cdn.example.com/lei1.pdf").encode(
            "utf-8"
        )
        result = scraper._extract_docs_from_soup(html)
        assert result[0]["pdf_link"] == "https://cdn.example.com/lei1.pdf"

    def test_multiple_rows(self):
        scraper = _make_scraper()
        html = (
            _make_row_html(
                title="Lei 1/2023", doc_link="/detalhe?id=1", pdf_href="/a1.pdf"
            )
            + _make_row_html(
                title="Lei 2/2023", doc_link="/detalhe?id=2", pdf_href="/a2.pdf"
            )
        ).encode("utf-8")
        result = scraper._extract_docs_from_soup(html)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _extract_total_count
# ---------------------------------------------------------------------------


class TestExtractTotalCount:
    def test_no_registros_div_returns_none(self):
        soup = BeautifulSoup(
            "<html><body><p>Sem resultados</p></body></html>", "html.parser"
        )
        result = TocantinsScraper._extract_total_count(soup)
        assert result is None

    def test_extracts_count_from_strong(self):
        html = (
            "<html><body>"
            "<div>Registros encontrados: <strong>42</strong></div>"
            "</body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        result = TocantinsScraper._extract_total_count(soup)
        assert result == 42

    def test_ignores_div_without_strong(self):
        html = "<html><body><div>Registros encontrados: sem strong</div></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        result = TocantinsScraper._extract_total_count(soup)
        assert result is None


# ---------------------------------------------------------------------------
# _has_table_artifacts
# ---------------------------------------------------------------------------


class TestHasTableArtifacts:
    def test_empty_string_returns_false(self):
        assert TocantinsScraper._has_table_artifacts("") is False

    def test_text_without_pipes_returns_false(self):
        assert TocantinsScraper._has_table_artifacts("Texto normal sem pipes.") is False

    def test_many_pipes_returns_true(self):
        # ≥ 80 pipes required; "| a | b | c | d |" = 5 pipes × 20 repetitions = 100
        text = "| a | b | c | d |" * 20
        assert TocantinsScraper._has_table_artifacts(text) is True

    def test_many_pipe_lines_returns_true(self):
        text = "\n".join(["| cell | cell |"] * 10)
        assert TocantinsScraper._has_table_artifacts(text) is True

    def test_few_pipes_returns_false(self):
        text = "| one |"  # only 2 pipes
        assert TocantinsScraper._has_table_artifacts(text) is False


# ---------------------------------------------------------------------------
# _normalize_table_markdown
# ---------------------------------------------------------------------------


class TestNormalizeTableMarkdown:
    def test_removes_separator_rows(self):
        text = "| col1 | col2 |\n|---|---|\n| val1 | val2 |"
        result = TocantinsScraper._normalize_table_markdown(text)
        assert "---|---" not in result
        assert "val1" in result

    def test_collapses_multiple_newlines(self):
        text = "Line 1.\n\n\n\n\nLine 2."
        result = TocantinsScraper._normalize_table_markdown(text)
        assert "\n\n\n" not in result

    def test_non_pipe_lines_preserved(self):
        text = "Normal text here.\nAnother line."
        result = TocantinsScraper._normalize_table_markdown(text)
        assert "Normal text here." in result
        assert "Another line." in result

    def test_pipe_cells_joined_with_spaces(self):
        text = "| Alpha | Beta | Gamma |"
        result = TocantinsScraper._normalize_table_markdown(text)
        assert "Alpha" in result
        assert "Beta" in result
        assert "Gamma" in result
        assert "|" not in result


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    def _make_doc(self, **kwargs):
        base = {
            "pdf_link": "https://www.al.to.leg.br/arquivos/lei1.pdf",
            "title": "Lei Ordinária 1/2023",
            "year": 2023,
            "summary": "Ementa.",
            "situation": "Não consta",
            "date": "01/01/2023",
        }
        base.update(kwargs)
        return base

    @pytest.mark.asyncio
    async def test_missing_pdf_link_returns_none(self):
        scraper = _make_scraper()
        scraper._save_doc_error = AsyncMock()
        doc = self._make_doc()
        doc.pop("pdf_link")
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_already_scraped_returns_none(self):
        scraper = _make_scraper()
        pdf_link = "https://www.al.to.leg.br/arquivos/lei1.pdf"
        scraper._scraped_keys = {(pdf_link, "Lei Ordinária 1/2023")}
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_failed_request_returns_none_and_saves_error(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_returns_none_and_saves_error(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=b"%PDF fake")
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_whitespace_markdown_returns_none(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=b"%PDF fake")
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        scraper._get_markdown = AsyncMock(return_value="   \n\n   ")
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_valid_markdown_returns_correct_shape(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        pdf_bytes = b"%PDF-1.4 content here"
        resp.read = AsyncMock(return_value=pdf_bytes)
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        valid_md = "# Lei Ordinária 1/2023\n\n" + "Texto da lei. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = self._make_doc()
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["text_markdown"] == valid_md
        assert result["document_url"] == "https://www.al.to.leg.br/arquivos/lei1.pdf"
        assert result["_raw_content"] == pdf_bytes
        assert result["_content_extension"] == ".pdf"

    @pytest.mark.asyncio
    async def test_pdf_link_removed_from_result(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=b"%PDF content")
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        valid_md = "# Lei\n\n" + "Texto. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = self._make_doc()
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert "pdf_link" not in result

    @pytest.mark.asyncio
    async def test_table_artifacts_normalized_before_validation(self):
        """Markdown with table artifacts is normalized before _valid_markdown check."""
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=b"%PDF content")
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        # Simulate table-heavy markdown that normalizes to valid text
        pipe_md = ("| Lei Ordinária | 1/2023 |\n" * 10) + "\n" + ("Texto normal. " * 30)
        normalized_md = "# Lei\n\n" + "Texto. " * 30
        scraper._get_markdown = AsyncMock(return_value=pipe_md)
        # Patch _normalize_table_markdown to return valid text
        scraper._normalize_table_markdown = MagicMock(return_value=normalized_md)
        doc = self._make_doc()
        await scraper._get_doc_data(doc)
        # Normalization should have been called since pipe_count > 80
        assert scraper._normalize_table_markdown.called
