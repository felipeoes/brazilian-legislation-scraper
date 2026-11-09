"Parser and metadata regression tests for rondonia_scraper."

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.ro import SITUATIONS, TYPES, RondoniaCotelScraper


def _make_scraper(**kwargs) -> RondoniaCotelScraper:
    """Instantiate RondoniaCotelScraper with injected dependencies."""
    return make_scraper(RondoniaCotelScraper, **kwargs)


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


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = list
    EXPECTED_EMPTY = True


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = RondoniaCotelScraper
    STATE_NAME = "Rondônia"


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


def _make_list_soup(rows_html: str) -> BeautifulSoup:
    """Wrap rows in the expected table structure."""
    html = f'<html><body><table id="ContentPlaceHolder1_DataList1"><tbody>{rows_html}</tbody></table></body></html>'
    return BeautifulSoup(html, "lxml")


def _make_valid_row(
    title: str = "Lei Ordinária 1/2023",
    doc_id: str = "42",
    summary: str = "Dispõe sobre algo.",
    pdf_filename: str = "leiord1.pdf",
    detail_href: str = "/COTEL/Livros/detalhes.aspx?id=42",
) -> str:
    return f'<tr><td><div><a href="{detail_href}">{title}</a><a href="/COTEL/Livros/Files/{pdf_filename}">{pdf_filename}</a><span id="ementadocLabel">{summary}</span><span id="coddocLabel">{doc_id}</span></div></td></tr>'
