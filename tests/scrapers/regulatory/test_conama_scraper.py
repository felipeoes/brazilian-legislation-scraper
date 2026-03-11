"""Tests for ConamaScraper.

Covers:
- TYPES constant completeness and documentation-only role
- _format_search_url: correct URL construction (no tipo param)
- _clean_dou_html: chrome removal, single-pass merge, garbage/pattern checks
- _clean_pdf_markdown: regex artefact removal without double _clean_markdown
- _get_doc_data: early-exit on None aid, resume skip, failed request,
  empty markdown, server-error string caught by _valid_markdown,
  invalid markdown (too short), correct result dict shape with type field
- _fetch_page_norms: happy-path and failure fallback
- _scrape_year (integration): total count matches API, type field sourced
  from nomeato, pagination guard, no tipo in requests

Run with:
    uv run pytest tests/test_conama_scraper.py -v
"""

import json
import re
from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.conama.scrape import TYPES, ConamaScraper
from base_tests import TypesConstantTests
from conftest import make_base_scraper, make_failed_request


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> ConamaScraper:
    """Instantiate ConamaScraper bypassing heavy __init__ (no network, no I/O)."""
    return make_base_scraper(
        ConamaScraper,
        "https://conama.mma.gov.br/",
        "CONAMA",
        TYPES,
        params={"option": "com_sisconama", "order": "asc", "limit": 100},
        _situation_regex=re.compile(r"Revogad|Revogação", re.IGNORECASE),
        **kwargs,
    )


def _make_api_response(rows: list[dict], total: int | None = None) -> MagicMock:
    """Mock aiohttp response returning a CONAMA-shaped JSON payload."""
    resp = MagicMock()
    resp.__bool__ = lambda s: True
    payload = {
        "data": {"rows": rows, "total": total if total is not None else len(rows)}
    }
    resp.json = AsyncMock(return_value=payload)
    resp.content_type = "application/json"
    return resp


def _make_http_response(
    body: bytes, content_type: str = "application/pdf"
) -> MagicMock:
    """Mock aiohttp response for a document download."""
    resp = MagicMock()
    resp.__bool__ = lambda s: True
    resp.read = AsyncMock(return_value=body)
    resp.content_type = content_type
    return resp


def _sample_row(**overrides) -> dict:
    base = {
        "id": 1,
        "id_tipo_ato": 1,
        "numero": "265",
        "ano": 2000,
        "descricao": "Dispõe sobre algo.",
        "status": "Vigente",
        "nomeato": "Resolução",
        "aid": 263,
        "porigem": None,
        "palavra_chave": None,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 6
    REQUIRED_KEYS = {
        "Resolução",
        "Moção",
        "Recomendação",
        "Proposição",
        "Decisão",
        "Portaria",
    }
    REQUIRE_INT_VALUES = True

    def test_resolucao_is_type_1(self):
        assert TYPES["Resolução"] == 1

    def test_portaria_is_type_6(self):
        assert TYPES["Portaria"] == 6


# ---------------------------------------------------------------------------
# _format_search_url
# ---------------------------------------------------------------------------


class TestFormatSearchUrl:
    def test_contains_no_tipo_parameter(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="2000")
        assert "tipo" not in url

    def test_contains_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="2000")
        assert "ano=2000" in url

    def test_contains_offset(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=100, year="2000")
        assert "offset=100" in url

    def test_contains_limit_100(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="2000")
        assert "limit=100" in url

    def test_contains_correct_task(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="1990")
        assert "task=atosnormativos.getList" in url

    def test_starts_with_base_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="2005")
        assert url.startswith("https://conama.mma.gov.br/")

    def test_default_offset_is_zero(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(year="2010")
        assert "offset=0" in url


# ---------------------------------------------------------------------------
# _clean_dou_html
# ---------------------------------------------------------------------------


class TestCleanDouHtml:
    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_removes_script_tags(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><script>alert(1)</script><p>texto</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert soup.find("script") is None
        assert "texto" in soup.get_text()

    def test_removes_title_tag(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><head><title>DOU - Imprensa Nacional</title></head><body><p>conteúdo</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert soup.find("title") is None

    def test_removes_element_with_dou_garbage_string(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><p>DOU - Imprensa Nacional</p><p>conteúdo legal</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "DOU - Imprensa Nacional" not in soup.get_text()
        assert "conteúdo legal" in soup.get_text()

    def test_does_not_remove_long_element_containing_garbage_string(self):
        """Elements >300 chars containing garbage should NOT be removed."""
        scraper = _make_scraper()
        long_text = "DOU - Imprensa Nacional " + ("texto legal " * 30)
        soup = self._soup(f"<html><body><p>{long_text}</p></body></html>")
        scraper._clean_dou_html(soup)
        # The paragraph is too long (>300 chars), so it should survive
        assert "texto legal" in soup.get_text()

    def test_removes_publicado_em_pattern(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><p>Publicado em: 01/01/2000</p><p>Art. 1°</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "Publicado em:" not in soup.get_text()
        assert "Art. 1°" in soup.get_text()

    def test_removes_orgao_pattern(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><span>Órgão: Ministério X</span><p>conteúdo</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "Órgão:" not in soup.get_text()

    def test_removes_timestamp_pattern(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><span>25/03/2020, 10:30</span><p>conteúdo</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "25/03/2020" not in soup.get_text()

    def test_removes_page_fraction_pattern(self):
        scraper = _make_scraper()
        soup = self._soup("<html><body><span>1/3</span><p>conteúdo</p></body></html>")
        scraper._clean_dou_html(soup)
        assert "1/3" not in soup.get_text()

    def test_removes_disclaimer_pattern(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><p>Este texto não substitui o publicado no DOU.</p><p>Art. 1°</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "não substitui" not in soup.get_text()

    def test_unwraps_non_dou_links(self):
        scraper = _make_scraper()
        soup = self._soup('<html><body><a href="/outro">Art. 1°</a></body></html>')
        scraper._clean_dou_html(soup)
        assert soup.find("a") is None
        assert "Art. 1°" in soup.get_text()

    def test_removes_in_gov_br_links(self):
        scraper = _make_scraper()
        soup = self._soup(
            '<html><body><a href="https://in.gov.br/materia/123">link</a></body></html>'
        )
        scraper._clean_dou_html(soup)
        assert soup.find("a") is None
        # Text also gone because the element is removed, not unwrapped
        assert "link" not in soup.get_text()

    def test_returns_soup_for_chaining(self):
        scraper = _make_scraper()
        soup = self._soup("<html><body><p>texto</p></body></html>")
        result = scraper._clean_dou_html(soup)
        assert result is soup


# ---------------------------------------------------------------------------
# _clean_pdf_markdown
# ---------------------------------------------------------------------------


class TestCleanPdfMarkdown:
    def test_removes_form_feed(self):
        scraper = _make_scraper()
        result = scraper._clean_pdf_markdown("page1\x0cpage2")
        assert "\x0c" not in result

    def test_removes_sei_artefact(self):
        scraper = _make_scraper()
        text = "Ato 123 (456)  SEI ABCDE12345 / pg. 7\nConteúdo real."
        result = scraper._clean_pdf_markdown(text)
        assert "SEI ABCDE12345" not in result
        assert "Conteúdo real." in result

    def test_removes_dou_date_artefact(self):
        scraper = _make_scraper()
        text = "25/03/2020, 14:30\nArt. 1°"
        result = scraper._clean_pdf_markdown(text)
        assert "25/03/2020" not in result
        assert "Art. 1°" in result

    def test_removes_disclaimer_pattern(self):
        scraper = _make_scraper()
        text = "Art. 1°\nEste texto não substitui o publicado no DOU."
        result = scraper._clean_pdf_markdown(text)
        assert "não substitui" not in result
        assert "Art. 1°" in result

    def test_removes_in_gov_br_url(self):
        scraper = _make_scraper()
        text = "Veja em https://www.in.gov.br/materia/12345 para mais detalhes."
        result = scraper._clean_pdf_markdown(text)
        assert "in.gov.br" not in result

    def test_does_not_double_clean_markdown_links(self):
        """_clean_pdf_markdown must NOT call _clean_markdown internally.
        _get_markdown already applies _clean_markdown; a second call would be
        redundant. We verify _clean_markdown is not invoked on the instance.
        """
        scraper = _make_scraper()
        scraper._clean_markdown = MagicMock(side_effect=lambda t, **kw: t)
        scraper._clean_pdf_markdown("some text")
        scraper._clean_markdown.assert_not_called()

    def test_preserves_legal_content(self):
        scraper = _make_scraper()
        legal = "Art. 1° Esta resolução estabelece critérios.\n§ 1° Aplica-se a todos."
        result = scraper._clean_pdf_markdown(legal)
        assert "Art. 1°" in result
        assert "§ 1°" in result


# ---------------------------------------------------------------------------
# _get_doc_data (unit — mocked network)
# ---------------------------------------------------------------------------


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_returns_none_for_null_aid(self):
        scraper = _make_scraper()
        row = _sample_row(aid=None)
        result = await scraper._get_doc_data(row)
        assert result is None

    @pytest.mark.asyncio
    async def test_null_aid_checked_before_resume(self):
        """Resume check must not be called when aid is None (no URL to check)."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        row = _sample_row(aid=None)
        await scraper._get_doc_data(row)
        scraper._is_already_scraped.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_already_scraped_document(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        row = _sample_row()
        result = await scraper._get_doc_data(row)
        assert result is None
        scraper.request_service.make_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_none_on_failed_request(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._get_doc_data(_sample_row())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_on_empty_markdown(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value="")
        result = await scraper._get_doc_data(_sample_row())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_on_server_error_in_markdown(self):
        """Server-error strings must be caught by _valid_markdown, not a separate check."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(b"%PDF-1.4 fake")
        )
        # Return a server-error string long enough to pass length check but caught by pattern
        server_error = "failed to open stream: HTTP request failed! " * 5
        scraper._get_markdown = AsyncMock(return_value=server_error)
        scraper._clean_pdf_markdown = MagicMock(return_value=server_error)
        result = await scraper._get_doc_data(_sample_row())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_on_too_short_markdown(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value="curto")
        scraper._clean_pdf_markdown = MagicMock(return_value="curto")
        result = await scraper._get_doc_data(_sample_row())
        assert result is None

    @pytest.mark.asyncio
    async def test_result_contains_type_from_nomeato(self):
        """The 'type' key must be sourced from nomeato, not from the context."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Esta resolução estabelece critérios ambientais. " * 10
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)
        scraper._clean_pdf_markdown = MagicMock(return_value=long_md)

        row = _sample_row(nomeato="Moção", numero="003", ano=2000, aid=377)
        result = await scraper._get_doc_data(row)

        assert result is not None
        assert result["type"] == "Moção"

    @pytest.mark.asyncio
    async def test_result_dict_has_required_keys(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo legal completo e detalhado. " * 10
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)
        scraper._clean_pdf_markdown = MagicMock(return_value=long_md)

        result = await scraper._get_doc_data(_sample_row())

        assert result is not None
        required = {
            "year",
            "title",
            "type",
            "id",
            "number",
            "summary",
            "situation",
            "keyword",
            "origin",
            "text_markdown",
            "document_url",
            "_raw_content",
            "_content_extension",
        }
        assert required.issubset(result.keys())

    @pytest.mark.asyncio
    async def test_revoked_status_sets_invalid_situation(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo legal revogado extenso. " * 10
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)
        scraper._clean_pdf_markdown = MagicMock(return_value=long_md)

        from src.scraper.base.scraper import DEFAULT_INVALID_SITUATION

        row = _sample_row(status="Revogada em 2005")
        result = await scraper._get_doc_data(row)

        assert result is not None
        assert result["situation"] == DEFAULT_INVALID_SITUATION

    @pytest.mark.asyncio
    async def test_active_status_sets_valid_situation(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo legal vigente e extenso. " * 10
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)
        scraper._clean_pdf_markdown = MagicMock(return_value=long_md)

        from src.scraper.base.scraper import DEFAULT_VALID_SITUATION

        row = _sample_row(status="Vigente")
        result = await scraper._get_doc_data(row)

        assert result is not None
        assert result["situation"] == DEFAULT_VALID_SITUATION

    @pytest.mark.asyncio
    async def test_html_content_type_path_raw_content_is_mhtml(self):
        """On the HTML path, raw_content must be the MHTML bytes from browser capture."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()

        original_body = (
            b"<html><body><p>Art. 1 Conteudo legal extenso e valido.</p></body></html>"
        )
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(original_body, content_type="text/html")
        )
        soup = BeautifulSoup(original_body, "html.parser")
        mhtml = b"MHTML content"
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        long_md = "Art. 1 Conteúdo legal extenso e válido. " * 10
        scraper._get_markdown = AsyncMock(return_value=long_md)

        result = await scraper._get_doc_data(_sample_row())

        assert result is not None
        assert result["_raw_content"] == mhtml
        assert result["_content_extension"] == ".mhtml"

    @pytest.mark.asyncio
    async def test_pdf_content_type_path_sets_pdf_extension(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo PDF extenso e válido. " * 10
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(
                b"%PDF-1.4 fake", content_type="application/pdf"
            )
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)
        scraper._clean_pdf_markdown = MagicMock(return_value=long_md)

        result = await scraper._get_doc_data(_sample_row())

        assert result is not None
        assert result["_content_extension"] == ".pdf"

    @pytest.mark.asyncio
    async def test_document_url_contains_aid(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo legal. " * 15
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_http_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)
        scraper._clean_pdf_markdown = MagicMock(return_value=long_md)

        row = _sample_row(aid=9999)
        result = await scraper._get_doc_data(row)

        assert result is not None
        assert "id=9999" in result["document_url"]


# ---------------------------------------------------------------------------
# _fetch_page_norms (unit — mocked network)
# ---------------------------------------------------------------------------


class TestFetchPageNorms:
    @pytest.mark.asyncio
    async def test_returns_rows_on_success(self):
        scraper = _make_scraper()
        rows = [_sample_row(numero=str(i)) for i in range(5)]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows)
        )
        result = await scraper._fetch_page_norms(offset=100, year_str="2000")
        assert result == rows

    @pytest.mark.asyncio
    async def test_returns_empty_on_failed_request(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._fetch_page_norms(offset=0, year_str="2000")
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_malformed_json(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"unexpected": "shape"})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._fetch_page_norms(offset=0, year_str="2000")
        assert result == []

    @pytest.mark.asyncio
    async def test_url_passed_to_request_contains_offset_and_year(self):
        scraper = _make_scraper()
        rows = [_sample_row()]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows)
        )
        await scraper._fetch_page_norms(offset=200, year_str="1998")
        call_url = scraper.request_service.make_request.call_args[0][0]
        assert "offset=200" in call_url
        assert "ano=1998" in call_url
        assert "tipo" not in call_url


# ---------------------------------------------------------------------------
# _scrape_year (unit — mocked network)
# ---------------------------------------------------------------------------


class TestScrapeYearUnit:
    @pytest.mark.asyncio
    async def test_returns_empty_on_failed_first_request(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._scrape_year(2000)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_malformed_json(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"bad": "response"})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._scrape_year(2000)
        assert result == []

    @pytest.mark.asyncio
    async def test_no_pagination_when_total_fits_one_page(self):
        """When total <= limit, _fetch_page_norms must not be called."""
        scraper = _make_scraper()
        rows = [_sample_row(numero=str(i), aid=i) for i in range(10)]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows, total=10)
        )
        scraper._process_documents = AsyncMock(return_value=[])
        scraper._fetch_page_norms = AsyncMock(return_value=[])

        await scraper._scrape_year(2000)

        scraper._fetch_page_norms.assert_not_called()

    @pytest.mark.asyncio
    async def test_pagination_triggered_when_total_exceeds_limit(self):
        """When total > 100, additional pages must be fetched."""
        scraper = _make_scraper()
        rows = [_sample_row(numero=str(i), aid=i) for i in range(100)]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows, total=150)
        )
        scraper._fetch_page_norms = AsyncMock(
            return_value=[_sample_row(numero="999", aid=999)]
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(2000)

        scraper._fetch_page_norms.assert_awaited_once_with(100, "2000")

    @pytest.mark.asyncio
    async def test_process_documents_called_with_all_rows(self):
        scraper = _make_scraper()
        rows = [_sample_row(numero=str(i), aid=i) for i in range(5)]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows, total=5)
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(2000)

        call_docs = scraper._process_documents.call_args[0][0]
        assert len(call_docs) == 5

    @pytest.mark.asyncio
    async def test_single_request_made_per_year_when_no_pagination(self):
        """Exactly one HTTP request for a year with total <= limit."""
        scraper = _make_scraper()
        rows = [_sample_row()]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows, total=1)
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(1984)

        assert scraper.request_service.make_request.call_count == 1

    @pytest.mark.asyncio
    async def test_request_url_has_no_tipo_param(self):
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response([], total=0)
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(2005)

        call_url = scraper.request_service.make_request.call_args[0][0]
        assert "tipo" not in call_url


# ---------------------------------------------------------------------------
# Integration tests — live API
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_scrape_year_2000_count_matches_api(integration_scraper_factory):
    """Total documents returned must match the API-reported total for year 2000."""
    async with integration_scraper_factory(
        ConamaScraper,
        year_start=2000,
        year_end=2000,
        overwrite=True,
        rps=10,
    ) as scraper:
        await scraper._load_scraped_keys(2000)

        # Get expected total from the API directly
        url = scraper._format_search_url(offset=0, year="2000")
        resp = await scraper.request_service.make_request(url)
        assert resp, "Could not reach CONAMA API"
        payload = await resp.json(content_type=None)
        api_total = payload["data"]["total"]
        print(f"\nAPI reports {api_total} norms for year 2000")

        results = await scraper._scrape_year(2000)
        print(f"Scraper returned {len(results)} results")

        # Allow a small tolerance for documents that fail (empty text, 404, etc.)
        tolerance = max(2, int(api_total * 0.05))
        assert len(results) >= api_total - tolerance, (
            f"Expected ~{api_total} docs, got {len(results)} (tolerance={tolerance})"
        )


@pytest.mark.integration
async def test_scrape_year_2000_type_field_from_nomeato(integration_scraper_factory):
    """Every result must have a non-empty 'type' field sourced from nomeato."""
    async with integration_scraper_factory(
        ConamaScraper,
        year_start=2000,
        year_end=2000,
        overwrite=True,
        rps=10,
    ) as scraper:
        await scraper._load_scraped_keys(2000)
        results = await scraper._scrape_year(2000)
        assert results, "No results returned"

        missing_type = [d for d in results if not d.get("type")]
        assert missing_type == [], (
            f"{len(missing_type)} docs missing type field: "
            + ", ".join(d.get("title", "?") for d in missing_type[:3])
        )

        # Types must all be known CONAMA types
        unknown_types = {d["type"] for d in results if d["type"] not in TYPES}
        assert unknown_types == set(), f"Unknown types found: {unknown_types}"


@pytest.mark.integration
async def test_scrape_year_2000_no_tipo_in_requests(integration_scraper_factory):
    """No request made during a full year scrape should contain 'tipo='."""
    async with integration_scraper_factory(
        ConamaScraper,
        year_start=2000,
        year_end=2000,
        overwrite=True,
        rps=10,
    ) as scraper:
        await scraper._load_scraped_keys(2000)

        made_urls: list[str] = []
        original_make_request = scraper.request_service.make_request

        async def tracking_make_request(url, **kwargs):
            made_urls.append(url)
            return await original_make_request(url, **kwargs)

        scraper.request_service.make_request = tracking_make_request
        await scraper._scrape_year(2000)

        listing_urls = [u for u in made_urls if "atosnormativos.getList" in u]
        tipo_urls = [u for u in listing_urls if "tipo=" in u]
        assert tipo_urls == [], f"Found listing requests with tipo= param: {tipo_urls}"


@pytest.mark.integration
async def test_scrape_year_2000_mixed_types_in_single_request(
    integration_scraper_factory,
):
    """Year 2000 contains both Resolução and Moção — both must appear in one request."""
    async with integration_scraper_factory(
        ConamaScraper,
        year_start=2000,
        year_end=2000,
        overwrite=True,
        rps=10,
    ) as scraper:
        await scraper._load_scraped_keys(2000)
        results = await scraper._scrape_year(2000)

        types_found = {d["type"] for d in results}
        assert "Resolução" in types_found, "Expected Resoluções in year 2000"
        assert "Moção" in types_found, "Expected Moções in year 2000"


@pytest.mark.integration
async def test_scrape_year_2000_saved_docs_have_situation(integration_scraper_factory):
    """Every saved document must have a non-empty situation field."""
    async with integration_scraper_factory(
        ConamaScraper,
        year_start=2000,
        year_end=2000,
        overwrite=True,
        rps=10,
    ) as scraper:
        await scraper._load_scraped_keys(2000)
        results = await scraper._scrape_year(2000)
        assert results, "No results returned"

        # Check shard files written by FileSaver
        save_dir = scraper.docs_save_dir
        shard_files = list(save_dir.rglob("chunk_*.json"))
        saved_docs = []
        for sf in shard_files:
            content = json.loads(sf.read_text(encoding="utf-8"))
            docs = (
                content if isinstance(content, list) else content.get("documents", [])
            )
            saved_docs.extend(docs)

        missing_situation = [d for d in saved_docs if not d.get("situation")]
        assert missing_situation == [], (
            f"{len(missing_situation)} saved docs missing situation: "
            + ", ".join(d.get("title", "?") for d in missing_situation[:3])
        )
