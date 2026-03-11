"""Tests for MTAlmtScraper (Mato Grosso).

Covers:
- TYPES constant: 7 types, all integer IDs
- HISTORIC_TYPES: 8 historic types with string url_suffix IDs
- SITUATIONS constant: empty dict (situations come from API)
- Class docstring accessible (__doc__ is not None)
- BeautifulSoup `string=` (not deprecated `text=`) used for strong tag lookups
- _build_search_url: regular vs historic, correct params, no shared state mutation
- _get_total_norms: empty soup returns 0, regex extracts count correctly
- _extract_docs_from_soup: extracts type from title, skips items with < 2 links
- _get_docs_links: thin wrapper — happy-path returns docs, failed soup returns []
- _get_doc_data: resume skip, ficha error, compilado error, no turbo-frame, invalid markdown
  → error + None; valid → correct shape with .html extension

Run with:
    .venv/bin/pytest tests/test_mato_grosso_scraper.py -v
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.mato_grosso import (
    HISTORIC_TYPES,
    SITUATIONS,
    TYPES,
    MTAlmtScraper,
)
from base_tests import TypesConstantTests, SituationsConstantTests, ScraperClassTests
from conftest import make_base_scraper, assert_resume_skips


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> MTAlmtScraper:
    """Instantiate MTAlmtScraper bypassing __init__ (no network, no I/O)."""
    import re

    return make_base_scraper(
        MTAlmtScraper,
        "https://www.al.mt.gov.br",
        "MATO_GROSSO",
        dict(TYPES),
        situations=dict(SITUATIONS),
        historic_types=dict(HISTORIC_TYPES),
        max_year_historic=1978,
        min_year=1979,
        token="test_token",
        regex_total_items=re.compile(r"Total de registros:\s+([\d.]+)"),
        **kwargs,
    )


def _make_valid_md() -> str:
    return "# Lei Estadual de Mato Grosso\n\nO governador do estado decreta. " * 30


def _make_docs_html(count: int = 3, is_historic: bool = False) -> str:
    """Build a search-results HTML fragment with `count` norm items."""
    prefix = (
        ""
        if is_historic
        else """
        <div class="col-12"><h5>Form</h5><div class="text-muted">dummy</div></div>
        <div class="col-12"><h5>Filter</h5><div class="text-muted">dummy</div></div>"""
    )
    items = prefix
    for i in range(1, count + 1):
        items += f"""
        <div class="col-12">
            <h5>Lei Ordinária - {i:04d}/2020</h5>
            <div class="text-muted">Ementa da lei {i}</div>
            <a href="/norma-juridica/{i}/download">PDF</a>
            <a href="/norma-juridica/{i}">Ver norm</a>
        </div>"""
    # Append a pagination div to be stripped
    items += '<div class="col-12"><nav>pagination</nav></div>'
    return f"<html><body><p>Total de registros: {count}</p>{items}</body></html>"


def _make_total_html(total: int = 5) -> str:
    return f"<html><body><p>Total de registros: {total}</p></body></html>"


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 7
    REQUIRED_KEYS = {"Constituição Estadual", "Lei Ordinária"}
    REQUIRE_INT_VALUES = True


# ---------------------------------------------------------------------------
# HISTORIC_TYPES constant
# ---------------------------------------------------------------------------


class TestHistoricTypesConstant:
    def test_has_8_types(self):
        assert len(HISTORIC_TYPES) == 8

    def test_ids_are_strings(self):
        for name, type_id in HISTORIC_TYPES.items():
            assert isinstance(type_id, str), f"{name} id not str"

    def test_lei_ordinaria_present(self):
        assert "Lei Ordinária" in HISTORIC_TYPES
        assert HISTORIC_TYPES["Lei Ordinária"] == "lei-ordinaria"


# ---------------------------------------------------------------------------
# SITUATIONS constant
# ---------------------------------------------------------------------------


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = MTAlmtScraper
    STATE_NAME = "Mato Grosso"

    def test_situations_empty_by_default(self):
        scraper = _make_scraper()
        assert scraper.situations == {}

    def test_uses_string_not_text_for_beautifulsoup(self):
        """Verify that `text=` (deprecated) is not used; `string=` is used instead."""
        import ast
        import inspect

        source = inspect.getsource(MTAlmtScraper)
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for keyword in node.keywords:
                    if keyword.arg == "text":
                        if (
                            isinstance(node.func, ast.Attribute)
                            and node.func.attr == "find"
                        ):
                            pytest.fail(
                                f"Deprecated `text=` keyword found in soup.find() at line {node.lineno}. "
                                "Use `string=` instead."
                            )


# ---------------------------------------------------------------------------
# _build_search_url
# ---------------------------------------------------------------------------


class TestBuildSearchUrl:
    def test_regular_contains_ano_param(self):
        scraper = _make_scraper()
        url = scraper._build_search_url("", 2020, 1, is_historic=False)
        assert (
            "ano%5D=2020" in url
            or "ano]=2020" in url
            or "ano%5D%5D=2020" in url
            or "2020" in url
        )

    def test_historic_contains_tipo_param(self):
        scraper = _make_scraper()
        url = scraper._build_search_url("lei-ordinaria", 1970, 1, is_historic=True)
        assert "lei-ordinaria" in url
        assert "pesquisa-historica" in url

    def test_regular_url_uses_norma_juridica(self):
        scraper = _make_scraper()
        url = scraper._build_search_url("", 2020, 1, is_historic=False)
        assert "norma-juridica" in url
        assert "pesquisa-historica" not in url

    def test_empty_type_accepted_for_regular(self):
        scraper = _make_scraper()
        url = scraper._build_search_url("", 2020, 1, is_historic=False)
        assert "norma-juridica" in url

    def test_empty_type_accepted_for_historic(self):
        scraper = _make_scraper()
        url = scraper._build_search_url("", 1970, 1, is_historic=True)
        assert "pesquisa-historica" in url

    def test_no_shared_state_mutation(self):
        scraper = _make_scraper()
        url1 = scraper._build_search_url("", 2020, 1, is_historic=False)
        url2 = scraper._build_search_url("", 2019, 2, is_historic=False)
        assert "2020" in url1
        assert "2019" in url2


# ---------------------------------------------------------------------------
# _get_total_norms
# ---------------------------------------------------------------------------


class TestGetTotalNorms:
    def test_empty_soup_returns_zero(self):
        scraper = _make_scraper()
        assert scraper._get_total_norms(None) == 0

    def test_extracts_total_from_html(self):
        scraper = _make_scraper()
        html = _make_total_html(42)
        soup = BeautifulSoup(html, "html.parser")
        assert scraper._get_total_norms(soup) == 42

    def test_handles_dotted_thousands(self):
        scraper = _make_scraper()
        html = "<html><body><p>Total de registros: 1.234</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert scraper._get_total_norms(soup) == 1234

    def test_returns_zero_when_not_found(self):
        scraper = _make_scraper()
        html = "<html><body><p>No matching norms</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert scraper._get_total_norms(soup) == 0


# ---------------------------------------------------------------------------
# _extract_docs_from_soup
# ---------------------------------------------------------------------------


class TestExtractDocsFromSoup:
    def test_extracts_type_from_title(self):
        scraper = _make_scraper()
        html = _make_docs_html(2, is_historic=False)
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 2
        for doc in docs:
            assert doc["type"] == "Lei Ordinária"

    def test_type_empty_when_no_dash_in_title(self):
        scraper = _make_scraper()
        html = """<html><body>
            <div class="col-12"><h5>dummy form</h5><div class="text-muted">x</div></div>
            <div class="col-12"><h5>dummy filter</h5><div class="text-muted">x</div></div>
            <div class="col-12">
                <h5>SemTipoAqui</h5>
                <div class="text-muted">Ementa</div>
                <a href="/download/1">PDF</a>
                <a href="/norma/1">Ver</a>
            </div>
            <div class="col-12"><nav>pag</nav></div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 1
        assert docs[0]["type"] == ""

    def test_known_prefix_without_dash_is_inferred(self):
        scraper = _make_scraper()
        html = """<html><body>
            <div class="col-12"><h5>dummy form</h5><div class="text-muted">x</div></div>
            <div class="col-12"><h5>dummy filter</h5><div class="text-muted">x</div></div>
            <div class="col-12">
                <h5>Lei Ordinária 123/2020</h5>
                <div class="text-muted">Ementa</div>
                <a href="/download/1">PDF</a>
                <a href="/norma/1">Ver</a>
            </div>
            <div class="col-12"><nav>pag</nav></div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 1
        assert docs[0]["type"] == "Lei Ordinária"

    def test_skips_items_with_fewer_than_2_links(self):
        scraper = _make_scraper()
        html = """<html><body>
            <div class="col-12"><h5>Form</h5><div class="text-muted">x</div></div>
            <div class="col-12"><h5>Filter</h5><div class="text-muted">x</div></div>
            <div class="col-12">
                <h5>Lei Ordinária - 0001/2020</h5>
                <div class="text-muted">Ementa</div>
                <a href="/only-link">Link</a>
            </div>
            <div class="col-12"><nav>pag</nav></div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert docs == []

    def test_historic_does_not_skip_first_two(self):
        scraper = _make_scraper()
        html = """<html><body>
            <div class="col-12">
                <h5>Lei Ordinária - 0001/1970</h5>
                <div class="text-muted">Ementa 1</div>
                <a href="/download/1">PDF</a>
                <a href="/norma/1">Ver</a>
            </div>
            <div class="col-12">
                <h5>Lei Ordinária - 0002/1970</h5>
                <div class="text-muted">Ementa 2</div>
                <a href="/download/2">PDF</a>
                <a href="/norma/2">Ver</a>
            </div>
            <div class="col-12"><nav>pag</nav></div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._extract_docs_from_soup(soup, is_historic=True)
        assert len(docs) == 2

    def test_result_contains_required_keys(self):
        scraper = _make_scraper()
        html = _make_docs_html(1, is_historic=False)
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 1
        for key in ("title", "type", "summary", "norm_link", "document_url"):
            assert key in docs[0], f"Missing key: {key}"

    def test_document_url_is_absolute(self):
        scraper = _make_scraper()
        html = _make_docs_html(1, is_historic=False)
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert docs[0]["document_url"].startswith("https://www.al.mt.gov.br")

    def test_document_url_is_norm_page_not_pdf(self):
        """document_url must point to the canonical norm page (links[-1]), not the PDF link."""
        scraper = _make_scraper()
        html = _make_docs_html(1, is_historic=False)
        soup = BeautifulSoup(html, "html.parser")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 1
        # links[-1] is "/norma-juridica/1", links[0] is "/norma-juridica/1/download"
        assert "/download" not in docs[0]["document_url"]
        assert docs[0]["document_url"] == "https://www.al.mt.gov.br/norma-juridica/1"


# ---------------------------------------------------------------------------
# _get_docs_links
# ---------------------------------------------------------------------------


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_happy_path_returns_docs(self):
        scraper = _make_scraper()
        html = _make_docs_html(2, is_historic=False)
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_docs_links("https://example.com", is_historic=False)
        assert len(result) == 2
        for doc in result:
            assert "title" in doc
            assert "type" in doc
            assert "summary" in doc
            assert "norm_link" in doc
            assert "document_url" in doc

    @pytest.mark.asyncio
    async def test_failed_soup_returns_empty(self):
        scraper = _make_scraper()
        failed = MagicMock()
        failed.__bool__ = MagicMock(return_value=False)
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        result = await scraper._get_docs_links("https://example.com")
        assert result == []

    @pytest.mark.asyncio
    async def test_delegates_to_extract_docs_from_soup(self):
        scraper = _make_scraper()
        html = _make_docs_html(1, is_historic=True)
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        scraper._extract_docs_from_soup = MagicMock(return_value=[{"title": "x"}])
        result = await scraper._get_docs_links("https://example.com", is_historic=True)
        scraper._extract_docs_from_soup.assert_called_once_with(soup, True)
        assert result == [{"title": "x"}]


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


# Shared doc_info factory for _get_doc_data tests
def _make_doc_info():
    return {
        "title": "Lei 001/2020",
        "year": 2020,
        "document_url": "https://www.al.mt.gov.br/norma-juridica/urn:lex:br;mato.grosso:estadual:lei.ordinaria:2020-12-30;11281",
        "norm_link": "/norma-juridica/urn:lex:br;mato.grosso:estadual:lei.ordinaria:2020-12-30;11281",
        "summary": "Ementa",
    }


def _make_ficha_soup() -> BeautifulSoup:
    return BeautifulSoup(
        "<html><body><ul>"
        "<li><strong>Situação:</strong> Vigente</li>"
        "<li><strong>Publicação:</strong> 01/01/2020</li>"
        "<li><strong>Data da promulgação:</strong> 30/12/2020</li>"
        "</ul></body></html>",
        "html.parser",
    )


def _make_compilado_soup(with_frame: bool = True) -> BeautifulSoup:
    frame = (
        '<turbo-frame id="compilado"><p>Artigo 1. O governador decreta.</p></turbo-frame>'
        if with_frame
        else "<div>No frame here</div>"
    )
    return BeautifulSoup(f"<html><body>{frame}</body></html>", "html.parser")


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(_make_scraper(), _make_doc_info())

    @pytest.mark.asyncio
    async def test_ficha_failed_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        failed = MagicMock()
        failed.__bool__ = MagicMock(return_value=False)
        # Both calls return falsy; ficha check fires first
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        result = await scraper._get_doc_data(_make_doc_info())
        assert result is None
        scraper._save_doc_error.assert_called_once()
        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert call_kwargs.get("error_message") == "Failed to get document page"

    @pytest.mark.asyncio
    async def test_compilado_failed_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        failed = MagicMock()
        failed.__bool__ = MagicMock(return_value=False)
        # First call (ficha) succeeds; second call (compilado) fails
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[_make_ficha_soup(), failed]
        )
        result = await scraper._get_doc_data(_make_doc_info())
        assert result is None
        scraper._save_doc_error.assert_called_once()
        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert call_kwargs.get("error_message") == "Failed to get compilado page"

    @pytest.mark.asyncio
    async def test_compilado_no_turbo_frame_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[_make_ficha_soup(), _make_compilado_soup(with_frame=False)]
        )
        result = await scraper._get_doc_data(_make_doc_info())
        assert result is None
        scraper._save_doc_error.assert_called_once()
        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert "turbo-frame" in call_kwargs.get("error_message", "")

    @pytest.mark.asyncio
    async def test_invalid_markdown_saves_error(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper._strip_html_chrome = MagicMock()
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._capture_mhtml = AsyncMock(return_value=b"fake-mhtml")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[_make_ficha_soup(), _make_compilado_soup()]
        )
        result = await scraper._get_doc_data(_make_doc_info())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper._strip_html_chrome = MagicMock()
        scraper._get_markdown = AsyncMock(return_value=_make_valid_md())
        scraper._capture_mhtml = AsyncMock(return_value=b"fake-mhtml")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[_make_ficha_soup(), _make_compilado_soup()]
        )
        result = await scraper._get_doc_data(_make_doc_info())
        assert result is not None
        assert "# Lei Estadual" in result["text_markdown"]
        assert result["_content_extension"] == ".mhtml"
        assert result["_raw_content"] == b"fake-mhtml"
        assert "norm_link" not in result  # popped
        assert result.get("situation") == "Vigente"
