"Parser and metadata regression tests for rio_grande_do_norte_scraper."

import pytest
from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.rn import SITUATIONS, TYPES, RNAlrnScraper


def _make_scraper(**kwargs) -> RNAlrnScraper:
    """Instantiate RNAlrnScraper with injected dependencies."""
    return make_scraper(RNAlrnScraper, **kwargs)


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


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = RNAlrnScraper
    STATE_NAME = "Rio Grande do Norte"


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


class TestInferNormType:
    def test_matches_titles_without_space_before_number(self):
        assert (
            RNAlrnScraper._infer_norm_type("Lei Ordinária12.029,de 30.12.2024")
            == "Lei Ordinária"
        )

    def test_matches_lei_complementar_abbreviation(self):
        assert (
            RNAlrnScraper._infer_norm_type("Lei Comp12,de 2024") == "Lei Complementar"
        )

    def test_unknown_prefix_falls_back_to_title_prefix(self):
        assert (
            RNAlrnScraper._infer_norm_type("Norma Especial99,de 2024")
            == "Norma Especial"
        )


def _make_table_soup(rows_html: str) -> BeautifulSoup:
    html = f'<html><body><table class="table table-sm table-striped">{rows_html}</table></body></html>'
    return BeautifulSoup(html, "lxml")


def _make_row(title: str, year: int, pdf_href: str) -> str:
    return (
        f'<tr><th>{title}</th><td>{year}</td><td><a href="{pdf_href}">PDF</a></td></tr>'
    )


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_no_table_returns_empty(self):
        """When the table element is not found, return [] instead of crashing."""
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>Sem tabela</p></body></html>", "lxml")
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
