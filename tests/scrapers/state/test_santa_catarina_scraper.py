"Parser and metadata regression tests for santa_catarina_scraper."

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.sc import SITUATIONS, TYPES, SantaCatarinaScraper


def _make_scraper(**kwargs) -> SantaCatarinaScraper:
    """Instantiate SantaCatarinaScraper with injected dependencies."""
    return make_scraper(SantaCatarinaScraper, **kwargs)


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 2
    REQUIRED_KEYS = {"Legislativo", "Executivo"}
    REQUIRE_INT_VALUES = False


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = list
    EXPECTED_EMPTY = True


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = SantaCatarinaScraper
    STATE_NAME = "Santa Catarina"


class TestFormatSearchUrl:
    def test_includes_path(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("legislativo", 2023)
        assert "legislativo" in url

    def test_includes_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("legislativo", 2023)
        assert "ano=2023" in url

    def test_starts_with_base_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("executivo", 2020)
        assert url.startswith("https://leis.alesc.sc.gov.br")

    def test_executivo_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("executivo", 2022)
        assert url == "https://leis.alesc.sc.gov.br/executivo?ano=2022"


def _make_link_soup(links_html: str) -> BeautifulSoup:
    html = f"<html><body>{links_html}</body></html>"
    return BeautifulSoup(html, "lxml")


def _make_card_html(
    href: str = "/ato-normativo/legislativo/123",
    title: str = "Lei 1/2023",
    ementa: str = "Dispõe sobre algo.",
) -> str:
    return f'<div class="card-item"><a href="{href}">{title}</a><div class="ementa">{ementa}</div></div>'
