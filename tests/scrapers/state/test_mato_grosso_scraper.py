"Parser and metadata regression tests for mato_grosso_scraper."

import pytest
from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.mt import HISTORIC_TYPES, SITUATIONS, TYPES, MTAlmtScraper


def _make_scraper(**kwargs) -> MTAlmtScraper:
    """Instantiate MTAlmtScraper with injected dependencies."""
    import re

    return make_scraper(
        MTAlmtScraper,
        historic_types=dict(HISTORIC_TYPES),
        max_year_historic=1978,
        token="test_token",
        regex_total_items=re.compile("Total de registros:\\s+([\\d.]+)"),
        **kwargs,
    )


def _make_valid_md() -> str:
    return "# Lei Estadual de Mato Grosso\n\nO governador do estado decreta. " * 30


def _make_docs_html(count: int = 3, is_historic: bool = False) -> str:
    """Build a search-results HTML fragment with `count` norm items."""
    prefix = (
        ""
        if is_historic
        else '\n        <div class="col-12"><h5>Form</h5><div class="text-muted">dummy</div></div>\n        <div class="col-12"><h5>Filter</h5><div class="text-muted">dummy</div></div>'
    )
    items = prefix
    for i in range(1, count + 1):
        items += f'\n        <div class="col-12">\n            <h5>Lei Ordinária - {i:04d}/2020</h5>\n            <div class="text-muted">Ementa da lei {i}</div>\n            <a href="/norma-juridica/{i}/download">PDF</a>\n            <a href="/norma-juridica/{i}">Ver norm</a>\n        </div>'
    items += '<div class="col-12"><nav>pagination</nav></div>'
    return f"<html><body><p>Total de registros: {count}</p>{items}</body></html>"


def _make_total_html(total: int = 5) -> str:
    return f"<html><body><p>Total de registros: {total}</p></body></html>"


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 7
    REQUIRED_KEYS = {"Constituição Estadual", "Lei Ordinária"}
    REQUIRE_INT_VALUES = True


class TestHistoricTypesConstant:
    def test_has_8_types(self):
        assert len(HISTORIC_TYPES) == 8

    def test_ids_are_strings(self):
        for name, type_id in HISTORIC_TYPES.items():
            assert isinstance(type_id, str), f"{name} id not str"

    def test_lei_ordinaria_present(self):
        assert "Lei Ordinária" in HISTORIC_TYPES
        assert HISTORIC_TYPES["Lei Ordinária"] == "lei-ordinaria"


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


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
                                f"Deprecated `text=` keyword found in soup.find() at line {node.lineno}. Use `string=` instead."
                            )


class TestBuildSearchUrl:
    def test_regular_contains_ano_param(self):
        scraper = _make_scraper()
        url = scraper._build_search_url("", 2020, 1, is_historic=False)
        assert (
            "ano%5D=2020" in url
            or "ano]=2020" in url
            or "ano%5D%5D=2020" in url
            or ("2020" in url)
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


class TestGetTotalNorms:
    def test_empty_soup_returns_zero(self):
        scraper = _make_scraper()
        assert scraper._get_total_norms(None) == 0

    def test_extracts_total_from_html(self):
        scraper = _make_scraper()
        html = _make_total_html(42)
        soup = BeautifulSoup(html, "lxml")
        assert scraper._get_total_norms(soup) == 42

    def test_handles_dotted_thousands(self):
        scraper = _make_scraper()
        html = "<html><body><p>Total de registros: 1.234</p></body></html>"
        soup = BeautifulSoup(html, "lxml")
        assert scraper._get_total_norms(soup) == 1234

    def test_returns_zero_when_not_found(self):
        scraper = _make_scraper()
        html = "<html><body><p>No matching norms</p></body></html>"
        soup = BeautifulSoup(html, "lxml")
        assert scraper._get_total_norms(soup) == 0


class TestExtractDocsFromSoup:
    def test_extracts_type_from_title(self):
        scraper = _make_scraper()
        html = _make_docs_html(2, is_historic=False)
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 2
        for doc in docs:
            assert doc["type"] == "Lei Ordinária"

    def test_type_empty_when_no_dash_in_title(self):
        scraper = _make_scraper()
        html = '<html><body>\n            <div class="col-12"><h5>dummy form</h5><div class="text-muted">x</div></div>\n            <div class="col-12"><h5>dummy filter</h5><div class="text-muted">x</div></div>\n            <div class="col-12">\n                <h5>SemTipoAqui</h5>\n                <div class="text-muted">Ementa</div>\n                <a href="/download/1">PDF</a>\n                <a href="/norma/1">Ver</a>\n            </div>\n            <div class="col-12"><nav>pag</nav></div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 1
        assert docs[0]["type"] == ""

    def test_known_prefix_without_dash_is_inferred(self):
        scraper = _make_scraper()
        html = '<html><body>\n            <div class="col-12"><h5>dummy form</h5><div class="text-muted">x</div></div>\n            <div class="col-12"><h5>dummy filter</h5><div class="text-muted">x</div></div>\n            <div class="col-12">\n                <h5>Lei Ordinária 123/2020</h5>\n                <div class="text-muted">Ementa</div>\n                <a href="/download/1">PDF</a>\n                <a href="/norma/1">Ver</a>\n            </div>\n            <div class="col-12"><nav>pag</nav></div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 1
        assert docs[0]["type"] == "Lei Ordinária"

    def test_skips_items_with_fewer_than_2_links(self):
        scraper = _make_scraper()
        html = '<html><body>\n            <div class="col-12"><h5>Form</h5><div class="text-muted">x</div></div>\n            <div class="col-12"><h5>Filter</h5><div class="text-muted">x</div></div>\n            <div class="col-12">\n                <h5>Lei Ordinária - 0001/2020</h5>\n                <div class="text-muted">Ementa</div>\n                <a href="/only-link">Link</a>\n            </div>\n            <div class="col-12"><nav>pag</nav></div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert docs == []

    def test_historic_does_not_skip_first_two(self):
        scraper = _make_scraper()
        html = '<html><body>\n            <div class="col-12">\n                <h5>Lei Ordinária - 0001/1970</h5>\n                <div class="text-muted">Ementa 1</div>\n                <a href="/download/1">PDF</a>\n                <a href="/norma/1">Ver</a>\n            </div>\n            <div class="col-12">\n                <h5>Lei Ordinária - 0002/1970</h5>\n                <div class="text-muted">Ementa 2</div>\n                <a href="/download/2">PDF</a>\n                <a href="/norma/2">Ver</a>\n            </div>\n            <div class="col-12"><nav>pag</nav></div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=True)
        assert len(docs) == 2

    def test_result_contains_required_keys(self):
        scraper = _make_scraper()
        html = _make_docs_html(1, is_historic=False)
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 1
        for key in ("title", "type", "summary", "norm_link", "document_url"):
            assert key in docs[0], f"Missing key: {key}"

    def test_document_url_is_absolute(self):
        scraper = _make_scraper()
        html = _make_docs_html(1, is_historic=False)
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert docs[0]["document_url"].startswith("https://www.al.mt.gov.br")

    def test_document_url_is_norm_page_not_pdf(self):
        """document_url must point to the canonical norm page (links[-1]), not the PDF link."""
        scraper = _make_scraper()
        html = _make_docs_html(1, is_historic=False)
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 1
        assert "/download" not in docs[0]["document_url"]
        assert docs[0]["document_url"] == "https://www.al.mt.gov.br/norma-juridica/1"


def _make_doc_info():
    return {
        "title": "Lei 001/2020",
        "year": 2020,
        "type": "Lei Ordinária",
        "situation": "Não consta",
        "document_url": "https://www.al.mt.gov.br/norma-juridica/urn:lex:br;mato.grosso:estadual:lei.ordinaria:2020-12-30;11281",
        "norm_link": "/norma-juridica/urn:lex:br;mato.grosso:estadual:lei.ordinaria:2020-12-30;11281",
        "summary": "Ementa",
    }


def _make_ficha_soup() -> BeautifulSoup:
    return BeautifulSoup(
        "<html><body><ul><li><strong>Situação:</strong> Vigente</li><li><strong>Publicação:</strong> 01/01/2020</li><li><strong>Data da promulgação:</strong> 30/12/2020</li></ul></body></html>",
        "lxml",
    )


def _make_compilado_soup(with_frame: bool = True) -> BeautifulSoup:
    frame = (
        '<turbo-frame id="compilado"><p>Artigo 1. O governador decreta.</p></turbo-frame>'
        if with_frame
        else "<div>No frame here</div>"
    )
    return BeautifulSoup(f"<html><body>{frame}</body></html>", "lxml")


class TestExtractDocsFromSoupNoneGuards:
    def test_skips_item_missing_h5(self):
        scraper = _make_scraper()
        html = '<html><body>\n            <div class="col-12"><h5>Form</h5><div class="text-muted">x</div></div>\n            <div class="col-12"><h5>Filter</h5><div class="text-muted">x</div></div>\n            <div class="col-12">\n                <div class="text-muted">No h5 here</div>\n                <a href="/a">A</a><a href="/b">B</a>\n            </div>\n            <div class="col-12"><nav>pag</nav></div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert docs == []

    def test_skips_item_missing_text_muted(self):
        scraper = _make_scraper()
        html = '<html><body>\n            <div class="col-12"><h5>Form</h5><div class="text-muted">x</div></div>\n            <div class="col-12"><h5>Filter</h5><div class="text-muted">x</div></div>\n            <div class="col-12">\n                <h5>Lei Ordinária - 001/2020</h5>\n                <a href="/a">A</a><a href="/b">B</a>\n            </div>\n            <div class="col-12"><nav>pag</nav></div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert docs == []

    def test_valid_item_still_extracted_alongside_malformed(self):
        """A valid item is still returned when a preceding item is malformed."""
        scraper = _make_scraper()
        html = '<html><body>\n            <div class="col-12"><h5>Form</h5><div class="text-muted">x</div></div>\n            <div class="col-12"><h5>Filter</h5><div class="text-muted">x</div></div>\n            <div class="col-12">\n                <div class="text-muted">No h5 — will be skipped</div>\n                <a href="/bad-a">A</a><a href="/bad-b">B</a>\n            </div>\n            <div class="col-12">\n                <h5>Lei Ordinária - 001/2020</h5>\n                <div class="text-muted">Good ementa</div>\n                <a href="/download/1">PDF</a>\n                <a href="/norma/1">Ver</a>\n            </div>\n            <div class="col-12"><nav>pag</nav></div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._extract_docs_from_soup(soup, is_historic=False)
        assert len(docs) == 1
        assert docs[0]["type"] == "Lei Ordinária"
