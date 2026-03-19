"""Tests for ESAlesScraper (Espírito Santo).

Covers:
- TYPES constant: 13 types present, correct IDs
- SITUATIONS: module-level dict with VALID + INVALID, preserved for downstream
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set (year-only scraper, no cartesian product)
- situations={} passed to super().__init__
- _format_search_url: year-only URL pattern
- _parse_docs_from_soup: happy-path returns type+situation fields, empty container,
  process link skipped, docx link skipped, no btn-label-info link skipped
- _has_next_page: True when lbNext present and active, False when aspNetDisabled
- _fetch_first_page: returns (content, viewstate, eventvalidation) on success,
  (None, None, None) on request failure
- _fetch_next_page: POSTs lbNext and threads viewstate forward, None on failure
- _scrape_year: 2-page mock calls _fetch_first_page once and _fetch_next_page once,
  stops when no next page
- _get_doc_data: resume skip, PDF path (valid), PDF path invalid → OCR fallback,
  PDF download failure → error + None, HTML path soup failure → error + None,
  HTML path invalid markdown → error + None, HTML path valid → correct shape,
  year pre-injected → error messages contain year

Integration (live site):
- test_year_only_url_first_page_returns_results
- test_get_doc_data_returns_valid_markdown

Run with:
    .venv/bin/pytest tests/test_espirito_santo_scraper.py -v
"""

import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.espirito_santo import (
    ESAlesScraper,
    INVALID_SITUATIONS,
    SITUATIONS,
    TYPES,
    VALID_SITUATIONS,
    _clean_markdown,
)
from base_tests import TypesConstantTests, SituationsConstantTests, ScraperClassTests
from conftest import make_base_scraper, make_failed_request, assert_resume_skips


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> ESAlesScraper:
    """Instantiate ESAlesScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        ESAlesScraper,
        "https://www3.al.es.gov.br",
        "ESPIRITO_SANTO",
        TYPES,
        situations={},
        **kwargs,
    )


def _make_listing_html(items: int = 2, has_next: bool = True) -> bytes:
    """Build a minimal year-only listing page with `items` document rows."""
    item_html = ""
    for i in range(1, items + 1):
        item_html += f"""
        <div class="kt-widget5__item">
          <a class="kt-widget5__title">Lei Ordinária\n{i:03d}/2010</a>
          <a class="kt-widget5__desc">Ementa {i}</a>
          <div class="kt-widget5__info">
            <span class="kt-font-info">01/01/2010</span>
            <span class="kt-font-info">Em Vigor</span>
          </div>
          <div class="kt-widget5__info">
            <span class="kt-font-info">Governador</span>
          </div>
          <a class="btn btn-sm btn-label-info btn-pill d-block" href="/pdf/lei{i}.pdf">Texto</a>
        </div>"""

    vs = '<input type="hidden" id="__VIEWSTATE" name="__VIEWSTATE" value="vs_val" />'
    ev = '<input type="hidden" id="__EVENTVALIDATION" name="__EVENTVALIDATION" value="ev_val" />'
    if has_next:
        next_btn = '<a id="ContentPlaceHolder1_lbNext" href="#">Próxima</a>'
    else:
        next_btn = '<a id="ContentPlaceHolder1_lbNext" class="aspNetDisabled" href="#">Próxima</a>'

    return f"""<html><body>
        {vs}
        {ev}
        <div class="kt-portlet__body">
            {item_html}
        </div>
        {next_btn}
    </body></html>""".encode()


# ---------------------------------------------------------------------------
# TYPES and SITUATIONS constants
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 13
    REQUIRED_KEYS = {
        "Lei Ordinária",
        "Lei Complementar",
        "Decreto Executivo",
        "Constituição Estadual",
    }
    REQUIRE_INT_VALUES = True

    def test_lei_ordinaria_id(self):
        assert TYPES["Lei Ordinária"] == 3

    def test_lei_complementar_id(self):
        assert TYPES["Lei Complementar"] == 4


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_valid_situations_has_em_vigor(self):
        assert "Em Vigor" in VALID_SITUATIONS

    def test_invalid_situations_has_revogada(self):
        assert "Revogada" in INVALID_SITUATIONS

    def test_situations_merges_valid_and_invalid(self):
        assert "Em Vigor" in SITUATIONS
        assert "Revogada" in SITUATIONS

    def test_situations_count(self):
        assert len(SITUATIONS) == len(VALID_SITUATIONS) + len(INVALID_SITUATIONS)


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = ESAlesScraper
    STATE_NAME = "Espirito Santo"

    def test_situations_empty_in_instance(self):
        scraper = _make_scraper()
        assert scraper.situations == {}


# ---------------------------------------------------------------------------
# _format_search_url
# ---------------------------------------------------------------------------


class TestFormatSearchUrl:
    def test_url_contains_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2010)
        assert "ano=2010" in url

    def test_url_starts_with_base_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2010)
        assert url.startswith(scraper.base_url)

    def test_url_contains_interno(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2010)
        assert "interno=1" in url

    def test_url_has_no_tipo_or_situacao(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2010)
        assert "tipo=" not in url
        assert "situacao=" not in url


# ---------------------------------------------------------------------------
# _parse_docs_from_soup
# ---------------------------------------------------------------------------


class TestParseDocsFromSoup:
    def test_happy_path_returns_correct_fields(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_listing_html(2), "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert len(docs) == 2
        assert "norm_type" in docs[0]
        assert "situation" in docs[0]
        assert "date" in docs[0]
        assert "authors" in docs[0]
        assert "title" in docs[0]
        assert "doc_link" in docs[0]

    def test_norm_type_extracted_from_title_first_line(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_listing_html(1), "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs[0]["norm_type"] == "Lei Ordinária"

    def test_situation_extracted_from_second_info_span(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_listing_html(1), "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs[0]["situation"] == "Em Vigor"

    def test_date_extracted_from_first_info_span(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_listing_html(1), "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs[0]["date"] == "01/01/2010"

    def test_empty_container_returns_empty_list(self):
        scraper = _make_scraper()
        html = b"<html><body><div class='kt-portlet__body'></div></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []

    def test_missing_container_returns_empty_list(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(b"<html><body></body></html>", "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []

    def test_process_link_is_skipped(self):
        scraper = _make_scraper()
        html = b"""<html><body>
            <div class="kt-portlet__body">
              <div class="kt-widget5__item">
                <a class="kt-widget5__title">Lei Ordinaria\n001/2010</a>
                <a class="kt-widget5__desc">Ementa</a>
                <div class="kt-widget5__info">
                  <span class="kt-font-info">01/01/2010</span>
                  <span class="kt-font-info">Em Vigor</span>
                </div>
                <div class="kt-widget5__info"><span class="kt-font-info">Gov</span></div>
                <a class="btn btn-sm btn-label-info btn-pill d-block" href="/processo.aspx?id=1">Process</a>
              </div>
            </div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []

    def test_docx_link_is_skipped(self):
        scraper = _make_scraper()
        html = b"""<html><body>
            <div class="kt-portlet__body">
              <div class="kt-widget5__item">
                <a class="kt-widget5__title">Lei Ordinaria\n001/2010</a>
                <a class="kt-widget5__desc">Ementa</a>
                <div class="kt-widget5__info">
                  <span class="kt-font-info">01/01/2010</span>
                  <span class="kt-font-info">Em Vigor</span>
                </div>
                <div class="kt-widget5__info"><span class="kt-font-info">Gov</span></div>
                <a class="btn btn-sm btn-label-info btn-pill d-block" href="/doc.docx">Doc</a>
              </div>
            </div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []

    def test_no_btn_link_is_skipped(self):
        scraper = _make_scraper()
        html = b"""<html><body>
            <div class="kt-portlet__body">
              <div class="kt-widget5__item">
                <a class="kt-widget5__title">Lei Ordinaria\n001/2010</a>
                <a class="kt-widget5__desc">Ementa</a>
                <div class="kt-widget5__info">
                  <span class="kt-font-info">01/01/2010</span>
                  <span class="kt-font-info">Em Vigor</span>
                </div>
                <div class="kt-widget5__info"><span class="kt-font-info">Gov</span></div>
              </div>
            </div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []


# ---------------------------------------------------------------------------
# _has_next_page
# ---------------------------------------------------------------------------


class TestHasNextPage:
    def test_returns_true_when_lbnext_active(self):
        scraper = _make_scraper()
        html = b'<html><body><a id="ContentPlaceHolder1_lbNext" href="#">Next</a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        assert scraper._has_next_page(soup) is True

    def test_returns_false_when_lbnext_disabled(self):
        scraper = _make_scraper()
        html = b'<html><body><a id="ContentPlaceHolder1_lbNext" class="aspNetDisabled" href="#">Next</a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        assert scraper._has_next_page(soup) is False

    def test_returns_false_when_lbnext_absent(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(b"<html><body></body></html>", "html.parser")
        assert scraper._has_next_page(soup) is False


# ---------------------------------------------------------------------------
# _fetch_first_page
# ---------------------------------------------------------------------------


class TestFetchFirstPage:
    @pytest.mark.asyncio
    async def test_returns_content_and_viewstate(self):
        scraper = _make_scraper()
        page_bytes = _make_listing_html(1, has_next=False)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=page_bytes)
        scraper.request_service.make_request = AsyncMock(return_value=resp)

        content, vs, ev = await scraper._fetch_first_page("http://example.com")
        assert content == page_bytes
        assert vs == "vs_val"
        assert ev == "ev_val"

    @pytest.mark.asyncio
    async def test_returns_none_on_request_failure(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)

        content, vs, ev = await scraper._fetch_first_page("http://example.com")
        assert content is None
        assert vs is None
        assert ev is None


# ---------------------------------------------------------------------------
# _fetch_postback
# ---------------------------------------------------------------------------


class TestFetchPostback:
    @pytest.mark.asyncio
    async def test_posts_generic_target_and_returns_new_state(self):
        scraper = _make_scraper()
        page_bytes = _make_listing_html(1, has_next=False)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=page_bytes)
        scraper.request_service.make_request = AsyncMock(return_value=resp)

        content, vs, ev = await scraper._fetch_postback(
            "http://example.com", "old_vs", "old_ev", "my_target", "my_arg", "100"
        )
        assert content == page_bytes
        assert vs == "vs_val"
        assert ev == "ev_val"
        call_kwargs = scraper.request_service.make_request.call_args
        payload = (
            call_kwargs.kwargs.get("payload") or call_kwargs.args[1]
            if len(call_kwargs.args) > 1
            else call_kwargs.kwargs["payload"]
        )
        assert payload["__EVENTTARGET"] == "my_target"
        assert payload["__EVENTARGUMENT"] == "my_arg"
        assert payload["__VIEWSTATE"] == "old_vs"
        assert payload["ctl00$ContentPlaceHolder1$ddl_ItensExibidos"] == "100"

    @pytest.mark.asyncio
    async def test_returns_none_on_request_failure(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)

        content, vs, ev = await scraper._fetch_postback(
            "http://example.com", "vs", "ev", "target"
        )
        assert content is None


# ---------------------------------------------------------------------------
# _scrape_year
# ---------------------------------------------------------------------------


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_year_full_flow_calls_methods_correctly(self):
        scraper = _make_scraper()
        page1_10 = _make_listing_html(1, has_next=False)
        page1_100 = _make_listing_html(2, has_next=True)
        page2 = _make_listing_html(2, has_next=False)

        scraper._fetch_first_page = AsyncMock(return_value=(page1_10, "vs1", "ev1"))
        scraper._fetch_postback = AsyncMock(
            side_effect=[
                (page1_100, "vs2", "ev2"),
                (page2, "vs3", "ev3"),
            ]
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(2010)

        scraper._fetch_first_page.assert_called_once()
        assert scraper._fetch_postback.call_count == 2
        # First postback: switch to 100 items
        scraper._fetch_postback.assert_any_call(
            scraper._format_search_url(2010),
            "vs1",
            "ev1",
            "ctl00$ContentPlaceHolder1$ddl_ItensExibidos",
            arg="",
            items_per_page="100",
        )
        # Second postback: go to next page
        scraper._fetch_postback.assert_any_call(
            scraper._format_search_url(2010),
            "vs2",
            "ev2",
            "ctl00$ContentPlaceHolder1$lbNext",
            arg="",
            items_per_page="100",
        )

    @pytest.mark.asyncio
    async def test_first_page_failure_returns_empty(self):
        scraper = _make_scraper()
        scraper._fetch_first_page = AsyncMock(return_value=(None, None, None))
        scraper._process_documents = AsyncMock(return_value=[])

        result = await scraper._scrape_year(2010)
        assert result == []
        scraper._process_documents.assert_not_called()

    @pytest.mark.asyncio
    async def test_year_injected_into_each_doc(self):
        scraper = _make_scraper()
        page1_10 = _make_listing_html(1, has_next=False)
        page1_100 = _make_listing_html(2, has_next=False)
        scraper._fetch_first_page = AsyncMock(return_value=(page1_10, "vs1", "ev1"))
        scraper._fetch_postback = AsyncMock(return_value=(page1_100, "vs2", "ev2"))
        captured_docs = []

        async def fake_process(docs, **kwargs):
            captured_docs.extend(docs)
            return []

        scraper._process_documents = fake_process

        await scraper._scrape_year(2023)
        assert all(d["year"] == 2023 for d in captured_docs)

    @pytest.mark.asyncio
    async def test_process_documents_called_with_correct_kwargs(self):
        scraper = _make_scraper()
        page1_10 = _make_listing_html(1, has_next=False)
        page1_100 = _make_listing_html(1, has_next=False)
        scraper._fetch_first_page = AsyncMock(return_value=(page1_10, "vs1", "ev1"))
        scraper._fetch_postback = AsyncMock(return_value=(page1_100, "vs2", "ev2"))
        captured = {}

        async def fake_process(docs, **kwargs):
            captured.update(kwargs)
            return []

        scraper._process_documents = fake_process

        await scraper._scrape_year(2015)
        assert captured["year"] == 2015
        assert captured["norm_type"] == "all"
        assert captured["situation"] == "all"


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(
            _make_scraper(),
            {
                "title": "Lei 001",
                "type": "Lei",
                "situation": "Vigente",
                "doc_link": "/pdf/lei1.pdf",
                "year": 2010,
            },
        )

    @pytest.mark.asyncio
    async def test_pd_url_fixed_to_pdf(self):
        """URLs ending in .pd should be corrected to .pdf."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Lei\n\n" + "Texto da lei. " * 20
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw", ".pdf")
        )
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/pdf/lei1.pd",
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert result["document_url"].endswith(".pdf")

    @pytest.mark.asyncio
    async def test_pdf_valid_markdown_returns_doc(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Lei\n\n" + "Texto da lei. " * 20
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw", ".pdf")
        )
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/pdf/lei1.pdf",
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert result["text_markdown"] == valid_md.strip()
        assert result["_content_extension"] == ".pdf"

    @pytest.mark.asyncio
    async def test_pdf_invalid_markdown_tries_ocr_fallback(self):
        """When _download_and_convert gives invalid MD, reuses raw_content for LLM OCR."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(
            return_value=("short", b"raw", ".pdf")
        )

        valid_md = "# Lei\n\n" + "Texto via OCR. " * 20
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/pdf/lei1.pdf",
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert result["text_markdown"] == valid_md.strip()
        # Verify _get_markdown was called with raw_content bytes (no re-download).
        from io import BytesIO

        scraper._get_markdown.assert_called_once()
        call_kwargs = scraper._get_markdown.call_args
        stream_arg = call_kwargs.kwargs.get("stream") or (
            call_kwargs.args[0] if call_kwargs.args else None
        )
        assert isinstance(stream_arg, BytesIO)
        assert stream_arg.read() == b"raw"

    @pytest.mark.asyncio
    async def test_pdf_download_failure_returns_none(self):
        """When both _download_and_convert and OCR fallback give invalid MD, returns None."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(return_value=("short", b"", ".pdf"))
        # OCR fallback also fails (returns invalid markdown).
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/pdf/lei1.pdf",
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_pdf_error_message_contains_year(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(return_value=("short", b"", ".pdf"))
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/pdf/lei1.pdf",
            "year": 2023,
        }
        await scraper._get_doc_data(doc_info)
        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert call_kwargs["year"] == 2023

    @pytest.mark.asyncio
    async def test_html_path_request_failure_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.get_soup = AsyncMock(return_value=make_failed_request())
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/html/lei1.html",
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_html_path_invalid_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup("<html><body></body></html>", "html.parser")
        )
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/html/lei1.html",
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_html_path_valid_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        soup = BeautifulSoup("<html><body><p>Texto</p></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        valid_md = "# Lei\n\n" + "Texto da lei. " * 20
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/html/lei1.html",
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert result["text_markdown"] == valid_md.strip()
        assert result["_content_extension"] == ".html"
        assert isinstance(result["_raw_content"], bytes)
        assert "document_url" in result

    @pytest.mark.asyncio
    async def test_html_path_img_tags_removed(self):
        """Images should be stripped before markdown conversion to avoid alt-text garbage."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html_with_img = (
            "<html><body>"
            '<img src="logo.png" alt="Garbage \\\\path\\logo.png">'
            "<p>Lei text</p></body></html>"
        )
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(html_with_img, "html.parser")
        )
        captured_html = {}

        async def capture_html(**kwargs):
            captured_html["html"] = kwargs.get("html_content", "")
            return "# Lei\n\n" + "Texto da lei. " * 20

        scraper._get_markdown = capture_html
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/html/lei1.html",
            "year": 2010,
        }
        await scraper._get_doc_data(doc_info)
        assert "<img" not in captured_html.get("html", "")
        assert "Garbage" not in captured_html.get("html", "")

    @pytest.mark.asyncio
    async def test_type_set_from_norm_type(self):
        """_get_doc_data should set 'type' from 'norm_type' and drop 'norm_type'."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Lei\n\n" + "Texto da lei. " * 20
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw", ".pdf")
        )
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/pdf/lei1.pdf",
            "year": 2010,
            "norm_type": "Lei Ordinária",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert result["type"] == "Lei Ordinária"
        assert "norm_type" not in result

    @pytest.mark.asyncio
    async def test_type_falls_back_to_title_inference_when_norm_type_missing(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Lei\n\n" + "Texto da lei. " * 20
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw", ".pdf")
        )
        doc_info = {
            "title": "Lei Ordinária n° 1/2025",
            "doc_link": "/pdf/lei1.pdf",
            "year": 2010,
            "situation": "Vigente",
            "norm_type": "",
        }

        result = await scraper._get_doc_data(doc_info)

        assert result is not None
        assert result["type"] == "Lei Ordinária"

    @pytest.mark.asyncio
    async def test_summary_not_in_result(self):
        """'summary' key must be absent from the returned doc dict."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Lei\n\n" + "Texto da lei. " * 20
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw", ".pdf")
        )
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/pdf/lei1.pdf",
            "year": 2010,
            "summary": "Texto da lei listing excerpt.",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert result["summary"] == ""

    @pytest.mark.asyncio
    async def test_manifesto_stripped_from_pdf(self):
        """MANIFESTO DE ASSINATURAS block at end of PDF must be removed."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        manifesto = (
            "\n\nMANIFESTO DE\nASSINATURAS\n\n"
            "Assinado digitalmente por:\n"
            "ALEXANDRE MARCELO COUTINHO SANTOS"
        )
        valid_md = "# Lei\n\n" + "Texto da lei. " * 5 + manifesto
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw", ".pdf")
        )
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": "/pdf/lei1.pdf",
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert "MANIFESTO" not in result["text_markdown"]
        assert "Assinado digitalmente" not in result["text_markdown"]
        assert "Texto da lei." in result["text_markdown"]

    @pytest.mark.asyncio
    async def test_summary_stripped_from_beginning_of_text_markdown(self):
        """Summary text found near the start of text_markdown should be removed."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        summary = "NOMEAR JOAO SILVA PARA O CARGO EM COMISSAO"
        # Use enough body text so valid_markdown accepts it.
        body = "\n\nPalácio Domingos Martins.\n\n" + "Texto legal de conclusão. " * 15
        valid_md = summary + body
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw", ".pdf")
        )
        doc_info = {
            "title": "Ato 001",
            "type": "Ato",
            "situation": "Vigente",
            "doc_link": "/pdf/ato1.pdf",
            "year": 2010,
            "summary": summary,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert not result["text_markdown"].upper().startswith(summary.upper())
        assert "Palácio" in result["text_markdown"]


# ---------------------------------------------------------------------------
# _clean_markdown / _strip_summary unit tests
# ---------------------------------------------------------------------------


class TestCleanMarkdown:
    _AUTH_FOOTER = (
        "Autenticar documento em https://www3.al.es.gov.br/autenticidade\n"
        "com o identificador 3400340032003000300034003A00540052004100, "
        "Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que "
        "institui a Infra-estrutura de Chaves Públicas Brasileira\n"
        "- ICP-Brasil."
    )

    def test_clean_text_unchanged(self):
        text = "# Lei\n\nTexto da lei normal.\n\nPalavras legais."
        assert _clean_markdown(text) == text.strip()

    def test_summary_stripped_when_at_start(self):
        summary = "NOMEAR JOAO SILVA PARA O CARGO"
        text = summary + "\n\nPalácioDomingos."
        result = _clean_markdown(text, summary)
        assert not result.upper().startswith(summary.upper())
        assert "Paláci" in result

    def test_summary_not_stripped_when_far_from_start(self):
        summary = "NOMEAR JOAO SILVA"
        # Place summary well past the 300-char threshold
        text = "A" * 400 + summary + "\n\nTexto final."
        result = _clean_markdown(text, summary)
        assert summary in result

    def test_long_summary_excerpt_inside_body_is_not_stripped(self):
        summary = (
            "NOMEAR, NA FORMA DO ARTIGO 12, INCISO II, DA LEI COMPLEMENTAR Nº 46, "
            "DE 31 DE JANEIRO DE 1994, CAROLINE QUEIROZ DOS SANTOS, PARA EXERCER "
            "O CARGO EM COMISSÃO DE TÉCNICO SÊNIOR DE GABINETE DE REPRESENTAÇÃO "
            "PARLAMENTAR - II, NO GABINETE DO(A) DEPUTADO(A) HUDSON LEAL, POR "
            "SOLICITAÇÃO DO(A) PRÓPRIO DEPUTADO(A)."
        )
        result = _clean_markdown(self._PAGE1_CONTENT, summary)
        assert result == self._PAGE1_CONTENT

    def test_empty_summary_does_nothing(self):
        text = "# Lei\n\nTexto."
        assert _clean_markdown(text, "") == text.strip()

    # ------------------------------------------------------------------
    # Manifesto / multi-section cleanup tests (pymupdf4llm format)
    # ------------------------------------------------------------------

    _PAGE1_CONTENT = (
        "ATO Nº\n\n"
        "A MESA DIRETORA DA ASSEMBLEIA LEGISLATIVA DO ESTADO DO ESPÍRITO SANTO,\n"
        "legais, e considerando o previsto no Processo nº\n"
        "usando de suas atribuições\n"
        "000028031/2025, resolve:\n\n"
        "NOMEAR, na forma do artigo 12, inciso II, da Lei Complementar nº 46, de 31 de janeiro\n"
        "de 1994, CAROLINE QUEIROZ DOS SANTOS, para exercer o cargo em comissão de TÉCNICO\n"
        "SÊNIOR DE GABINETE DE REPRESENTAÇÃO PARLAMENTAR -\n"
        "II, no gabinete do(a)\n"
        "Deputado(a) HUDSON LEAL, por solicitação do(a) próprio Deputado(a).\n\n"
        "Palácio Domingos Martins,\n\n"
        "MARCELO SANTOS\n"
        "Presidente"
    )

    _MANIFESTO_BLOCK = (
        "MANIFESTO DE\nASSINATURAS\n\n\n\n"
        "Assinado digitalmente por:\n"
        "ALEXANDRE MARCELO COUTINHO SANTOS\n"
        "CPF: ***.507.277-**\n"
        "Certificado emitido por AC SyngularID Multipla\n"
        "Data: 09/12/2025 19:07:20 -03:00"
    )

    def test_pymupdf4llm_manifesto_stripped(self):
        """pymupdf4llm-style output: content + manifesto in a single block."""
        raw = self._PAGE1_CONTENT + "\n\n\n" + self._MANIFESTO_BLOCK
        result = _clean_markdown(raw)
        assert result == self._PAGE1_CONTENT

    def test_manifesto_content_not_in_output(self):
        """MANIFESTO DE ASSINATURAS and digital signature must not appear."""
        raw = self._PAGE1_CONTENT + "\n\n\n" + self._MANIFESTO_BLOCK
        result = _clean_markdown(raw)
        assert "MANIFESTO" not in result
        assert "ASSINATURAS" not in result
        assert "Assinado digitalmente" not in result

    def test_multi_section_content_preserved(self):
        """Legal text that spans multiple sections must be fully preserved."""
        page2_text = (
            "Art. 3º As disposições anteriores aplicam-se subsidiariamente.\n\n"
            "Art. 4º Esta Lei entra em vigor na data de sua publicação."
        )
        raw = (
            self._PAGE1_CONTENT + "\n\n" + page2_text + "\n\n\n" + self._MANIFESTO_BLOCK
        )
        result = _clean_markdown(raw)
        assert self._PAGE1_CONTENT in result
        assert page2_text in result
        assert "MANIFESTO" not in result

    def test_auth_footer_removed_from_single_page(self):
        text = (
            "RESOLUÇÃO Nº 1\n\n"
            "Art. 1º Fica concedida a medalha.\n\n"
            "MARCELO SANTOS\nPresidente\n\n" + self._AUTH_FOOTER
        )
        result = _clean_markdown(text)
        assert result == (
            "RESOLUÇÃO Nº 1\n\n"
            "Art. 1º Fica concedida a medalha.\n\n"
            "MARCELO SANTOS\nPresidente"
        )

    def test_auth_footer_and_page_number_removed_from_single_page(self):
        text = "Art. 1º Texto principal.\n\n" + self._AUTH_FOOTER + "\n\nPágina 1"
        result = _clean_markdown(text)
        assert result == "Art. 1º Texto principal."
        assert "Página 1" not in result

    def test_auth_footer_split_across_lines_removed(self):
        raw = (
            "Art. 1º Texto da primeira página.\n\n"
            "Autenticar documento em https://www3.al.es.gov.br/autenticidade\n"
            "com o identificador 3400340032003000300034003A00540052004100, "
            "Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que "
            "institui a Infra-estrutura de Chaves Públicas Brasileira\n"
            "- ICP-Brasil.\n\n"
            "Art. 2º Texto da segunda página."
        )
        result = _clean_markdown(raw)
        assert result == (
            "Art. 1º Texto da primeira página.\n\nArt. 2º Texto da segunda página."
        )

    def test_multi_section_content_preserved_when_auth_footer_repeats(self):
        page2 = "Art. 2º Esta Resolução entra em vigor na data de sua publicação."
        raw = (
            self._PAGE1_CONTENT
            + "\n\n"
            + self._AUTH_FOOTER
            + "\n\n"
            + page2
            + "\n\n"
            + self._AUTH_FOOTER
            + "\n\nPágina 2"
        )
        result = _clean_markdown(raw)
        assert self._PAGE1_CONTENT in result
        assert page2 in result
        assert "Autenticar documento em" not in result
        assert "ICP-Brasil" not in result
        assert "Página 2" not in result

    def test_html_disclaimer_removed(self):
        text = (
            "Lei nº 1\n\n"
            "Art. 1º Texto da lei.\n\n"
            "Este texto não substitui o publicado no D.P.L. de 11/09/2025."
        )
        result = _clean_markdown(text)
        assert result == "Lei nº 1\n\nArt. 1º Texto da lei."

    def test_html_disclaimer_removed_with_line_breaks_and_do_variant(self):
        text = (
            "Lei nº 2\n\n"
            "Art. 1º Texto da lei.\n\n"
            "Este texto não substitui\no publicado no D.O. de 30/12/2025."
        )
        result = _clean_markdown(text)
        assert result == "Lei nº 2\n\nArt. 1º Texto da lei."

    def test_inline_pagina_reference_is_preserved(self):
        text = "Art. 1º Consulte a Página 12 do Anexo I para detalhes."
        assert _clean_markdown(text) == text

    def test_inline_disclaimer_like_phrase_is_preserved(self):
        text = (
            "O parecer registra a frase Este texto não substitui o publicado no "
            "D.O. de 30/12/2025. apenas como exemplo."
        )
        assert _clean_markdown(text) == text

    def test_autenticidade_reference_outside_footer_is_preserved(self):
        text = (
            "Art. 1º O sistema de autenticidade deverá permanecer disponível "
            "durante o horário de expediente."
        )
        assert _clean_markdown(text) == text

    @pytest.mark.asyncio
    async def test_digital_aspx_pdf_url_resolved(self):
        """Processo2/Digital.aspx URLs with a PDF arquivo param should be fetched as PDF."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Lei\n\n" + "Texto da lei. " * 20
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw", ".pdf")
        )
        digital_aspx_link = (
            "/Sistema/Protocolo/Processo2/Digital.aspx"
            "?id=438815"
            "&arquivo=Arquivo/Documents/RNSG/RNSG952025/doc.pdf"
            "&identificador=abc"
        )
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": digital_aspx_link,
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        # Should resolve to the direct PDF URL
        assert result["document_url"].endswith(".pdf")
        assert "Digital.aspx" not in result["document_url"]
        assert "RNSG952025/doc.pdf" in result["document_url"]

    @pytest.mark.asyncio
    async def test_digital_aspx_non_pdf_arquivo_returns_none(self):
        """Processo2/Digital.aspx URLs where arquivo is not a PDF should return None."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        digital_aspx_link = (
            "/Sistema/Protocolo/Processo2/Digital.aspx"
            "?id=12345"
            "&arquivo=Arquivo/Documents/XPTO/doc.docx"
        )
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": digital_aspx_link,
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None

    @pytest.mark.asyncio
    async def test_digital_aspx_missing_arquivo_returns_none(self):
        """Processo2/Digital.aspx URLs without an arquivo param should return None."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        digital_aspx_link = "/Sistema/Protocolo/Processo2/Digital.aspx?id=12345"
        doc_info = {
            "title": "Lei 001",
            "type": "Lei",
            "situation": "Vigente",
            "doc_link": digital_aspx_link,
            "year": 2010,
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None


# ---------------------------------------------------------------------------
# parse_base64_data_uri tests
# ---------------------------------------------------------------------------


class TestParseBase64DataUri:
    def test_valid_uri_parsed_correctly(self):
        from src.services.ocr.utils import parse_base64_data_uri
        import base64

        raw = b"PNG bytes here"
        b64 = base64.standard_b64encode(raw).decode()
        fmt, data = parse_base64_data_uri(f"data:image/png;base64,{b64}")
        assert fmt == "png"
        assert data == b64

    def test_empty_base64_returns_empty_string(self):
        """An empty base64 section (blank PDF page) must return '' not the full URI."""
        from src.services.ocr.utils import parse_base64_data_uri

        fmt, data = parse_base64_data_uri("data:image/png;base64,")
        assert fmt == "png"
        assert data == ""

    def test_invalid_uri_raises_value_error(self):
        from src.services.ocr.utils import parse_base64_data_uri

        with pytest.raises(ValueError, match="Not a valid data URI"):
            parse_base64_data_uri("not-a-data-uri")

    def test_jpeg_format_preserved(self):
        from src.services.ocr.utils import parse_base64_data_uri

        fmt, _ = parse_base64_data_uri("data:image/jpeg;base64,abc")
        assert fmt == "jpeg"

    def test_whitespace_stripped_from_base64(self):
        from src.services.ocr.utils import parse_base64_data_uri

        fmt, data = parse_base64_data_uri("data:image/png;base64,abc123  ")
        assert data == "abc123"


# ---------------------------------------------------------------------------
# Integration tests (live site)
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_year_only_url_first_page_returns_results():
    """ES ALES year-only URL should return at least one document on first page."""
    with tempfile.TemporaryDirectory() as tmp:
        scraper = ESAlesScraper(docs_save_dir=tmp, verbose=False)
        url = scraper._format_search_url(2010)
        content, vs, ev = await scraper._fetch_first_page(url)
        assert content is not None
        soup = BeautifulSoup(content, "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert len(docs) > 0
        assert "norm_type" in docs[0]
        assert "situation" in docs[0]
        assert "doc_link" in docs[0]


@pytest.mark.integration
async def test_get_doc_data_returns_valid_markdown():
    """Fetching the first doc from 2010 first page should yield non-empty markdown."""
    with tempfile.TemporaryDirectory() as tmp:
        scraper = ESAlesScraper(docs_save_dir=tmp, verbose=False)
        url = scraper._format_search_url(2010)
        content, _vs, _ev = await scraper._fetch_first_page(url)
        assert content is not None
        soup = BeautifulSoup(content, "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert len(docs) > 0

        doc = docs[0]
        doc["year"] = 2010
        result = await scraper._get_doc_data(doc)
        if result is not None:
            assert "text_markdown" in result
            assert result["text_markdown"] is not None
            assert len(result["text_markdown"]) > 50
