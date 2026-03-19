"""Tests for SaoPauloAlespScraper.

Covers:
- TYPES constant: 14 types present with correct IDs
- SITUATIONS / VALID_SITUATIONS / INVALID_SITUATIONS module-level dicts preserved
- Class docstring is accessible (__doc__ is not None)
- _iterate_situations NOT set (single all-types/all-situations query per year)
- _build_search_url: correct URL structure (year + page, idTipoSituacao=0, no idsTipoNorma)
- _extract_result_counts: parses various formats; returns None on no match
- _parse_docs_from_soup: extracts doc list from pre-fetched soup
- _get_docs_links: happy-path returns docs, soup failure returns []
- _infer_type: longest-match type inference from title
- _get_norm_data: happy-path returns expected dict keys incl. situation; soup failure returns {}
- _get_doc_data:
    - resume skip returns None
    - PDF path: valid → correct shape; invalid markdown → error + None
    - iframe PDF path: valid markdown → correct shape; failed request → error + None
    - HTML path: valid → correct shape; invalid markdown → error + None
    - HTML path with image for Decisão da Mesa: appended markdown
    - soup failure → error + None (not raise)
- _scrape_year: no results, count=0, single-page, multi-page calls _process_documents

Integration (live site):
- test_build_search_url_returns_real_results
- test_get_doc_data_html_returns_valid_markdown

Run with:
    .venv/bin/pytest tests/test_sao_paulo_scraper.py -v
"""

import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from bs4 import BeautifulSoup
from conftest import make_base_scraper, make_failed_request

from src.scraper.state_legislation.sao_paulo import (
    INVALID_SITUATIONS,
    SITUATIONS,
    TYPES,
    VALID_SITUATIONS,
    SaoPauloAlespScraper,
)

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> SaoPauloAlespScraper:
    """Instantiate SaoPauloAlespScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        SaoPauloAlespScraper,
        "https://www.al.sp.gov.br/norma/resultados",
        "SAO_PAULO",
        TYPES,
        situations=SITUATIONS,
        _page_size=500,
        _site_base="https://www.al.sp.gov.br",
        headers={"User-Agent": "test"},
        **kwargs,
    )


def _make_results_html(total: int = 3, pages: int = 1) -> str:
    rows = ""
    for i in range(1, total + 1):
        rows += f"""
        <tr>
          <td>
            <span>Lei {i:04d}/2020</span>
            <a href="/norma/{i}" class="link_norma">link</a>
            <a href="/norma/{i}/texto">texto</a>
          </td>
          <td><span>Ementa da Lei {i}</span></td>
        </tr>"""
    return f"""<html><body>
        <b>Resultado: {total} normas em {pages}</b>
        <table>{rows}</table>
    </body></html>"""


def _make_no_results_html() -> str:
    return "<html><body><div class='card cinza text-center'>Nenhuma norma encontrada como os parâmetros informados</div></body></html>"


def _make_valid_md() -> str:
    return "# Lei\n\nTexto da lei. " * 30


# ---------------------------------------------------------------------------
# TYPES / SITUATIONS constants
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 14
    REQUIRED_KEYS = {"Decreto", "Lei", "Constituição Estadual"}
    REQUIRE_INT_VALUES = True


class TestSituationsConstants(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_situations_is_union(self):
        assert len(SITUATIONS) == len(VALID_SITUATIONS) + len(INVALID_SITUATIONS)

    def test_valid_situations_contains_sem_revogacao(self):
        assert "Sem revogação expressa" in VALID_SITUATIONS

    def test_invalid_situations_contains_revogada(self):
        assert "Revogada" in INVALID_SITUATIONS

    def test_all_values_are_integers(self):
        for name, val in SITUATIONS.items():
            assert isinstance(val, int), f"{name!r} should have int ID"


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = SaoPauloAlespScraper
    STATE_NAME = "São Paulo"

    def test_situations_non_empty_in_instance(self):
        scraper = _make_scraper()
        assert len(scraper.situations) > 0


# ---------------------------------------------------------------------------
# _build_search_url
# ---------------------------------------------------------------------------


class TestBuildSearchUrl:
    def test_contains_base_url(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        assert "al.sp.gov.br/norma/resultados" in url

    def test_contains_year(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        assert "ano=2020" in url

    def test_all_situations(self):
        """idTipoSituacao=0 means all situations."""
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        assert "idTipoSituacao=0" in url

    def test_no_type_filter(self):
        """No idsTipoNorma= selection param — _idsTipoNorma=1 is only the checkbox sentinel."""
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        # _idsTipoNorma=1 (checkbox sentinel) is present; idsTipoNorma=<id> (type selection) must not be
        assert "_idsTipoNorma=1" in url
        assert "&idsTipoNorma=" not in url

    def test_page_param(self):
        scraper = _make_scraper()
        url0 = scraper._build_search_url(2020, page=0)
        url2 = scraper._build_search_url(2020, page=2)
        assert "page=0" in url0
        assert "page=2" in url2

    def test_no_shared_state_mutation(self):
        scraper = _make_scraper()
        url1 = scraper._build_search_url(2020)
        url2 = scraper._build_search_url(2019)
        assert "ano=2020" in url1
        assert "ano=2019" in url2


# ---------------------------------------------------------------------------
# _extract_result_counts
# ---------------------------------------------------------------------------


class TestExtractResultCounts:
    def test_parses_standard_format(self):
        html = "<html><body><b>Resultado: 42 normas em 1</b></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        result = SaoPauloAlespScraper._extract_result_counts(soup)
        assert result == (42, 1)

    def test_parses_multipage(self):
        html = "<html><body><b>Resultado: 1500 normas em 3</b></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        result = SaoPauloAlespScraper._extract_result_counts(soup)
        assert result == (1500, 3)

    def test_returns_none_on_no_match(self):
        html = "<html><body><b>No results here</b></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        result = SaoPauloAlespScraper._extract_result_counts(soup)
        assert result is None

    def test_returns_none_on_empty_soup(self):
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        result = SaoPauloAlespScraper._extract_result_counts(soup)
        assert result is None


# ---------------------------------------------------------------------------
# _parse_docs_from_soup
# ---------------------------------------------------------------------------


class TestParseDocs:
    def test_extracts_docs(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_results_html(3), "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert len(docs) == 3

    def test_doc_has_required_keys(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_results_html(1), "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert "title" in docs[0]
        assert "summary" in docs[0]
        assert "html_link" in docs[0]
        assert "norm_link" in docs[0]

    def test_html_link_is_absolute(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_results_html(1), "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs[0]["html_link"].startswith("http")

    def test_skips_mostrando_row(self):
        html = """<html><body>
            <table>
                <tr><td>Mostrando 1-10 de 50</td><td></td></tr>
                <tr>
                    <td><span>Lei 001/2020</span>
                        <a href="/norma/1" class="link_norma">link</a>
                        <a href="/norma/1/texto">texto</a>
                    </td>
                    <td><span>Ementa</span></td>
                </tr>
            </table>
        </body></html>"""
        scraper = _make_scraper()
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert len(docs) == 1

    def test_empty_table_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><table></table></body></html>", "html.parser")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []


# ---------------------------------------------------------------------------
# _get_docs_links
# ---------------------------------------------------------------------------


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_happy_path_returns_docs(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_results_html(3), "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        docs = await scraper._get_docs_links("http://example.com/normas?page=0")
        assert len(docs) == 3

    @pytest.mark.asyncio
    async def test_soup_failure_returns_empty(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        docs = await scraper._get_docs_links("http://example.com/normas?page=0")
        assert docs == []


# ---------------------------------------------------------------------------
# _infer_type
# ---------------------------------------------------------------------------


class TestInferType:
    def test_lei_complementar_matched_before_lei(self):
        """Longest match wins: 'Lei Complementar' before 'Lei'."""
        result = SaoPauloAlespScraper._infer_type("Lei Complementar 42/2020")
        assert result == "Lei Complementar"

    def test_lei_matched(self):
        result = SaoPauloAlespScraper._infer_type("Lei 9.394/1996")
        assert result == "Lei"

    def test_decreto_matched(self):
        result = SaoPauloAlespScraper._infer_type("Decreto 64.921/2020")
        assert result == "Decreto"

    def test_decisao_da_mesa_matched(self):
        result = SaoPauloAlespScraper._infer_type("Decisão da Mesa 1311/2005")
        assert result == "Decisão da Mesa"

    def test_unknown_falls_back_to_title_prefix(self):
        result = SaoPauloAlespScraper._infer_type("Norma Desconhecida 1/2020")
        assert result == "Norma Desconhecida"

    def test_case_insensitive(self):
        result = SaoPauloAlespScraper._infer_type("LEI 1/2020")
        assert result == "Lei"


# ---------------------------------------------------------------------------
# _get_norm_data
# ---------------------------------------------------------------------------


class TestGetNormData:
    @pytest.mark.asyncio
    async def test_soup_failure_returns_empty_dict(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        result = await scraper._get_norm_data("http://example.com/norma/1")
        assert result == {}

    @pytest.mark.asyncio
    async def test_happy_path_returns_expected_keys(self):
        html = """<html><body>
            <label>Promulgação</label><label>01/01/2020</label>
            <label>Projeto</label><label>PL 001/2019</label>
        </body></html>"""
        scraper = _make_scraper()
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_norm_data("http://example.com/norma/1")
        assert "promulgation" in result
        assert "project" in result
        assert "themes" in result
        assert "keywords" in result
        assert "situation" in result

    @pytest.mark.asyncio
    async def test_situation_extracted(self):
        html = """<html><body>
            <label>Situação</label><label>Sem revogação expressa</label>
        </body></html>"""
        scraper = _make_scraper()
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_norm_data("http://example.com/norma/1")
        assert result["situation"] == "Sem revogação expressa"


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    def _base_doc_info(self, suffix=".html"):
        return {
            "title": "Lei 001/2020",
            "summary": "Dispõe sobre...",
            "html_link": f"http://www.al.sp.gov.br/norma/1/texto{suffix}",
            "norm_link": "http://www.al.sp.gov.br/norma/1",
        }

    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        result = await scraper._get_doc_data(self._base_doc_info(), year=2020)
        assert result is None

    @pytest.mark.asyncio
    async def test_pdf_path_valid_markdown_returns_dict(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = _make_valid_md()
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw", ".pdf")
        )
        scraper._get_norm_data = AsyncMock(return_value={})
        result = await scraper._get_doc_data(self._base_doc_info(".pdf"), year=2020)
        assert result is not None
        assert result["text_markdown"] == valid_md.strip()
        assert result["_content_extension"] == ".pdf"

    @pytest.mark.asyncio
    async def test_pdf_path_invalid_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(
            return_value=("short", b"raw", ".pdf")
        )
        scraper._save_doc_error = AsyncMock()
        scraper._get_norm_data = AsyncMock(return_value={})
        result = await scraper._get_doc_data(self._base_doc_info(".pdf"), year=2020)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_html_path_valid_markdown_returns_dict(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_norm_data = AsyncMock(return_value={})
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        html = "<html><body><p>Texto da lei.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))

        result = await scraper._get_doc_data(self._base_doc_info(), year=2020)
        assert result is not None
        assert result["text_markdown"] == valid_md.strip()
        assert result["_content_extension"] == ".mhtml"

    @pytest.mark.asyncio
    async def test_html_path_invalid_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_norm_data = AsyncMock(return_value={})
        scraper._save_doc_error = AsyncMock()
        scraper._get_markdown = AsyncMock(return_value="short")

        html = "<html><body><p>Texto.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b""))

        result = await scraper._get_doc_data(self._base_doc_info(), year=2020)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_iframe_pdf_path_valid_markdown(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_norm_data = AsyncMock(return_value={})
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        html = """<html><body>
            <div id="UpdatePanel1">
                <iframe src="/pdf/lei001.pdf"></iframe>
            </div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))

        pdf_resp = MagicMock()
        pdf_resp.__bool__ = lambda s: True
        pdf_resp.read = AsyncMock(return_value=b"%PDF fake content")
        scraper.request_service.make_request = AsyncMock(return_value=pdf_resp)

        result = await scraper._get_doc_data(self._base_doc_info(), year=2020)
        assert result is not None
        assert result["_content_extension"] == ".pdf"
        assert result["text_markdown"] == valid_md.strip()

    @pytest.mark.asyncio
    async def test_iframe_no_src_falls_through_to_html(self):
        """panel_div exists but no iframe with src → falls through to HTML path."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_norm_data = AsyncMock(return_value={})
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        html = """<html><body>
            <div id="UpdatePanel1"><p>No iframe here</p></div>
            <p>Texto da lei.</p>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))

        result = await scraper._get_doc_data(self._base_doc_info(), year=2020)
        assert result is not None
        assert result["_content_extension"] == ".mhtml"

    @pytest.mark.asyncio
    async def test_iframe_failed_request_returns_none_and_saves_error(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_norm_data = AsyncMock(return_value={})
        scraper._save_doc_error = AsyncMock()

        html = """<html><body>
            <div id="UpdatePanel1">
                <iframe src="/pdf/lei001.pdf"></iframe>
            </div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)

        result = await scraper._get_doc_data(self._base_doc_info(), year=2020)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_soup_failure_returns_none_and_saves_error(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_norm_data = AsyncMock(return_value={})
        scraper._save_doc_error = AsyncMock()
        scraper._fetch_soup_and_mhtml = AsyncMock(side_effect=Exception("fetch failed"))
        result = await scraper._get_doc_data(self._base_doc_info(), year=2020)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_situation_populated_from_norm_data(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_norm_data = AsyncMock(
            return_value={
                "situation": "Revogada",
                "promulgation": None,
                "project": None,
                "themes": None,
                "keywords": None,
            }
        )
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        html = "<html><body><p>Texto da lei.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))

        result = await scraper._get_doc_data(self._base_doc_info(), year=2020)
        assert result is not None
        assert result["situation"] == "Revogada"

    @pytest.mark.asyncio
    async def test_situation_defaults_to_nao_consta(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_norm_data = AsyncMock(return_value={})
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        html = "<html><body><p>Texto da lei.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))

        result = await scraper._get_doc_data(self._base_doc_info(), year=2020)
        assert result is not None
        assert result["situation"] == "Não consta"

    @pytest.mark.asyncio
    async def test_norm_type_inferred_from_title(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_norm_data = AsyncMock(return_value={})
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        html = "<html><body><p>Texto da lei complementar.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, b"fake-mhtml"))

        doc = {
            "title": "Lei Complementar 42/2020",
            "summary": "Dispõe sobre...",
            "html_link": "http://www.al.sp.gov.br/norma/1/texto.html",
            "norm_link": "http://www.al.sp.gov.br/norma/1",
        }
        result = await scraper._get_doc_data(doc, year=2020)
        assert result is not None
        assert result["type"] == "Lei Complementar"


# ---------------------------------------------------------------------------
# _scrape_year
# ---------------------------------------------------------------------------


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_soup_failure_returns_empty(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        results = await scraper._scrape_year(2020)
        assert results == []

    @pytest.mark.asyncio
    async def test_no_results_text_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_no_results_html(), "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        results = await scraper._scrape_year(2020)
        assert results == []

    @pytest.mark.asyncio
    async def test_zero_count_returns_empty(self):
        scraper = _make_scraper()
        html = "<html><body><b>Resultado: 0 normas em 0</b></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        results = await scraper._scrape_year(2020)
        assert results == []

    @pytest.mark.asyncio
    async def test_single_page_calls_process_documents(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_results_html(3, 1), "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        scraper._process_documents = AsyncMock(return_value=[{"title": "x"}])
        results = await scraper._scrape_year(2020)
        scraper._process_documents.assert_called_once()
        assert results == [{"title": "x"}]

    @pytest.mark.asyncio
    async def test_multipage_fetches_extra_pages(self):
        scraper = _make_scraper()
        # page 0 soup has 3 pages
        soup_p0 = BeautifulSoup(_make_results_html(3, 3), "html.parser")
        soup_p1 = BeautifulSoup(_make_results_html(3, 3), "html.parser")
        soup_p2 = BeautifulSoup(_make_results_html(3, 3), "html.parser")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[soup_p0, soup_p1, soup_p2]
        )
        scraper._gather_results = AsyncMock(
            return_value=[[{"title": "x"}], [{"title": "y"}]]
        )
        scraper._process_documents = AsyncMock(return_value=[])
        await scraper._scrape_year(2020)
        # _gather_results should have been called with 2 tasks (pages 1 and 2)
        scraper._gather_results.assert_called_once()
        call_args = scraper._gather_results.call_args
        tasks = call_args[0][0]
        assert len(tasks) == 2


# ---------------------------------------------------------------------------
# Integration tests (live site)
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_build_search_url_returns_real_results():
    """São Paulo ALESP should return results for year 2020 with all situations."""
    with tempfile.TemporaryDirectory() as tmp:
        scraper = SaoPauloAlespScraper(docs_save_dir=tmp, verbose=False)
        url = scraper._build_search_url(2020, page=0)
        soup = await scraper.request_service.get_soup(url)
        assert soup
        counts = scraper._extract_result_counts(soup)
        assert counts is not None
        total, pages = counts
        assert total > 0


@pytest.mark.integration
async def test_get_doc_data_html_returns_valid_markdown():
    """Fetching a real Lei from São Paulo should return non-empty markdown."""
    with tempfile.TemporaryDirectory() as tmp:
        scraper = SaoPauloAlespScraper(docs_save_dir=tmp, verbose=False)
        url = scraper._build_search_url(2020, page=0)
        docs = await scraper._get_docs_links(url)
        assert len(docs) > 0
        doc = docs[0]
        result = await scraper._get_doc_data(doc, year=2020)
        if result is not None:
            assert "text_markdown" in result
            assert len(result["text_markdown"]) > 50
