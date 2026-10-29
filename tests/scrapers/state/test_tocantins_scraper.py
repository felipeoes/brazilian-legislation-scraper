"Parser and metadata regression tests for tocantins_scraper."

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.to import SITUATIONS, TYPES, TocantinsScraper


def _make_scraper(**kwargs) -> TocantinsScraper:
    """Instantiate TocantinsScraper with injected dependencies."""
    return make_scraper(
        TocantinsScraper,
        search_url="https://www.al.to.leg.br/legislacaoEstadual",
        **kwargs,
    )


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 2
    REQUIRED_KEYS = {"Lei Ordinária", "Lei Complementar"}
    REQUIRE_INT_VALUES = False


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = TocantinsScraper
    STATE_NAME = "Tocantins"


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


def _make_row_html(
    title: str = "Lei Ordinária 1/2023",
    doc_link: str = "/legislacaoEstadual/detalhe?id=1",
    date: str = "01/01/2023",
    summary: str = "Dispõe sobre algo.",
    pdf_href: str = "/arquivos/lei1.pdf",
) -> str:
    return f'<div class="row"><h4><a href="{doc_link}">{title}</a></h4><small>Data: {date}</small><em><strong>{summary}</strong></em><a href="{pdf_href}" title="Download">Download</a></div>'


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


class TestExtractTotalCount:
    def test_no_registros_div_returns_none(self):
        soup = BeautifulSoup("<html><body><p>Sem resultados</p></body></html>", "lxml")
        result = TocantinsScraper._extract_total_count(soup)
        assert result is None

    def test_extracts_count_from_strong(self):
        html = "<html><body><div>Registros encontrados: <strong>42</strong></div></body></html>"
        soup = BeautifulSoup(html, "lxml")
        result = TocantinsScraper._extract_total_count(soup)
        assert result == 42

    def test_ignores_div_without_strong(self):
        html = "<html><body><div>Registros encontrados: sem strong</div></body></html>"
        soup = BeautifulSoup(html, "lxml")
        result = TocantinsScraper._extract_total_count(soup)
        assert result is None


class TestHasTableArtifacts:
    def test_empty_string_returns_false(self):
        assert TocantinsScraper._has_table_artifacts("") is False

    def test_text_without_pipes_returns_false(self):
        assert TocantinsScraper._has_table_artifacts("Texto normal sem pipes.") is False

    def test_many_pipes_returns_true(self):
        text = "| a | b | c | d |" * 20
        assert TocantinsScraper._has_table_artifacts(text) is True

    def test_many_pipe_lines_returns_true(self):
        text = "\n".join(["| cell | cell |"] * 10)
        assert TocantinsScraper._has_table_artifacts(text) is True

    def test_few_pipes_returns_false(self):
        text = "| one |"
        assert TocantinsScraper._has_table_artifacts(text) is False


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
