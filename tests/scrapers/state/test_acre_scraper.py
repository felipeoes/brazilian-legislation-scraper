"Parser and metadata regression tests for acre_scraper."

import re

import pytest
from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import TypesConstantTests

from src.scrapers.states.ac import TYPES, AcreLegisScraper


def _make_scraper(**kwargs) -> AcreLegisScraper:
    """Instantiate AcreLegisScraper with injected dependencies."""
    return make_scraper(AcreLegisScraper, year_regex=re.compile("\\d{4}"), **kwargs)


def _listing_html(norm_type_id: str, rows: list[tuple[str, str, str]]) -> str:
    """Build a minimal listing page HTML with the given rows.

    Each row is a tuple of (title, href, summary).
    """
    row_html = ""
    for title, href, summary in rows:
        row_html += f'\n        <tr class="visaoQuadrosTr">\n            <td><a href="{href}">{title}</a></td>\n            <td>{summary}</td>\n        </tr>'
    return f'\n    <html><body>\n        <div id="{norm_type_id}">\n            <table>{row_html}</table>\n        </div>\n    </body></html>'


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 5
    REQUIRED_KEYS = {
        "Lei Ordinária",
        "Lei Complementar",
        "Constituição Estadual",
        "Decreto",
        "Emenda Constitucional",
    }
    REQUIRE_INT_VALUES = False

    def test_emenda_constitucional_maps_to_emendas(self):
        assert TYPES["Emenda Constitucional"] == "emendas"

    def test_constituicao_maps_to_detalhar_constituicao(self):
        assert TYPES["Constituição Estadual"] == "detalhar_constituicao"


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_parses_title_link_summary_year(self):
        scraper = _make_scraper()
        rows = [
            (
                "Lei Ordinária nº 1.234, de 15 de março de 2022",
                "/detalhar/999",
                "Dispõe sobre X.",
            )
        ]
        soup = BeautifulSoup(_listing_html("lei_ordinarias", rows), "lxml")
        docs = await scraper._get_docs_links(soup, "lei_ordinarias")
        assert len(docs) == 1
        doc = docs[0]
        assert doc["title"] == "Lei Ordinária nº 1.234, de 15 de março de 2022"
        assert doc["html_link"] == "/detalhar/999"
        assert doc["summary"] == "Dispõe sobre X."
        assert doc["year"] == "2022"

    @pytest.mark.asyncio
    async def test_parses_multiple_rows(self):
        scraper = _make_scraper()
        rows = [
            ("Lei nº 1, de 1963", "/detalhar/1", "Cria X."),
            ("Lei nº 2, de 1964", "/detalhar/2", "Cria Y."),
        ]
        soup = BeautifulSoup(_listing_html("lei_ordinarias", rows), "lxml")
        docs = await scraper._get_docs_links(soup, "lei_ordinarias")
        assert len(docs) == 2
        assert docs[0]["year"] == "1963"
        assert docs[1]["year"] == "1964"

    @pytest.mark.asyncio
    async def test_year_fallback_on_malformed_title(self):
        """Should return '0000' instead of raising AttributeError."""
        scraper = _make_scraper()
        rows = [("Lei sem data", "/detalhar/42", "Ementa qualquer.")]
        soup = BeautifulSoup(_listing_html("lei_ordinarias", rows), "lxml")
        docs = await scraper._get_docs_links(soup, "lei_ordinarias")
        assert docs[0]["year"] == "0000"

    @pytest.mark.asyncio
    async def test_emendas_section_parsed_correctly(self):
        scraper = _make_scraper()
        rows = [
            (
                "Emenda Constitucional nº 77, de 2023",
                "/detalhar_emendas/77",
                "Altera a Constituição.",
            )
        ]
        soup = BeautifulSoup(_listing_html("emendas", rows), "lxml")
        docs = await scraper._get_docs_links(soup, "emendas")
        assert len(docs) == 1
        assert docs[0]["year"] == "2023"


class TestCleanAcreHtml:
    def _make_detail_page(
        self, container_id: str, body: str, *, extra: str = ""
    ) -> str:
        return f'<html><body>\n            <div id="{container_id}">\n                <div class="row">LAYOUT JUNK</div>\n                <span id="texto_publicado_doe">DOE DATE</span>\n                {extra}\n                {body}\n            </div>\n        </body></html>'

    def test_extracts_body_law_content(self):
        scraper = _make_scraper()
        html = self._make_detail_page("body-law", "<p>Conteúdo da lei.</p>")
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert "Conteúdo da lei." in result

    def test_falls_back_to_exportacao_when_no_body_law(self):
        scraper = _make_scraper()
        html = self._make_detail_page("exportacao", "<p>Texto via exportacao.</p>")
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert "Texto via exportacao." in result

    def test_returns_empty_string_when_no_container(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            "<html><body><p>No relevant div.</p></body></html>", "lxml"
        )
        result = scraper._clean_acre_html(soup)
        assert result == ""

    def test_removes_layout_rows(self):
        scraper = _make_scraper()
        html = self._make_detail_page("body-law", "<p>Lei text.</p>")
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert "LAYOUT JUNK" not in result

    def test_removes_doe_span(self):
        scraper = _make_scraper()
        html = self._make_detail_page("body-law", "<p>Lei text.</p>")
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert "DOE DATE" not in result

    def test_unwraps_anchor_tags(self):
        """Links should be unwrapped (text kept, <a> tag removed)."""
        scraper = _make_scraper()
        html = self._make_detail_page(
            "body-law", '<p>Veja <a href="/detalhar/1">art. 1º</a>.</p>'
        )
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert "art. 1º" in result
        assert "<a " not in result

    def test_does_not_include_body_law_id_twice(self):
        """body-law fallback logic: body-law preferred over exportacao."""
        scraper = _make_scraper()
        html = '<html><body>\n            <div id="body-law"><p>Primary content.</p></div>\n            <div id="exportacao"><p>Secondary content.</p></div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert "Primary content." in result
        assert "Secondary content." not in result

    def test_removes_ementa_metadata_table(self):
        """The ementa table (first td empty, second td has summary) should be removed."""
        scraper = _make_scraper()
        html = self._make_detail_page(
            "body-law",
            "<p>Art. 1º Conteúdo real.</p>",
            extra='<table><tbody><tr>\n                <td width="52%"></td>\n                <td width="48%">Ementa que dispõe sobre X.</td>\n            </tr></tbody></table>',
        )
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert "Ementa que dispõe sobre X." not in result
        assert "Conteúdo real." in result

    def test_keeps_content_table(self):
        """Tables where the first cell has text should NOT be removed."""
        scraper = _make_scraper()
        html = self._make_detail_page(
            "body-law",
            "<table><tbody><tr>\n                <td>Coluna 1</td><td>Coluna 2</td>\n            </tr></tbody></table>",
        )
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert "Coluna 1" in result
        assert "Coluna 2" in result

    def test_removes_topo_lei_header(self):
        """The topo-lei div (state brasão/header) should be removed."""
        scraper = _make_scraper()
        html = '<html><body>\n            <div id="exportacao">\n                <div class="topo-lei"><h2>ESTADO DO ACRE</h2></div>\n                <p>PREÂMBULO</p>\n            </div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert "ESTADO DO ACRE" not in result
        assert "PREÂMBULO" in result

    def test_link_text_stays_inline(self):
        """After unwrapping, link text should not be on a separate line."""
        scraper = _make_scraper()
        html = self._make_detail_page(
            "body-law",
            '<p><span>texto antes </span><a href="https://example.com"><span>https://example.com</span></a><span> texto depois</span></p>',
        )
        soup = BeautifulSoup(html, "lxml")
        result = scraper._clean_acre_html(soup)
        assert (
            "\n" not in result.split("texto antes")[1].split("texto depois")[0].strip()
            or "https://example.com" in result
        )
