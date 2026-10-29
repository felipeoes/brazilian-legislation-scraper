"Parser and metadata regression tests for amazonas_scraper."

from unittest.mock import MagicMock

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.am import SITUATIONS, TYPES, LegislaAMScraper


def _make_scraper(**kwargs) -> LegislaAMScraper:
    """Instantiate LegislaAMScraper with injected dependencies."""
    return make_scraper(LegislaAMScraper, **kwargs)


def _make_listing_soup(
    items: list[tuple[str, str]], date_text: str = "Publicada em 04 de janeiro de 2025"
) -> BeautifulSoup:
    """Build a minimal listing page BeautifulSoup with (title, href) rows."""
    rows = ""
    for title, href in items:
        rows += f'<li class="item-li"><h5>{title}</h5><p>{date_text}</p><a href="{href}">texto</a></li>'
    html = f"<html><body><ul>{rows}</ul></body></html>"
    return BeautifulSoup(html, "lxml")


def _make_error_soup() -> BeautifulSoup:
    html = '<html><body><div id="container"><h1>Error</h1></div></body></html>'
    return BeautifulSoup(html, "lxml")


def _make_doc_soup(text: str = "A" * 200) -> BeautifulSoup:
    """Build a minimal document page BeautifulSoup with norm text."""
    html = f'<html><body><div class="materia rounded"><p>{text}</p></div></body></html>'
    return BeautifulSoup(html, "lxml")


def _make_response(soup: BeautifulSoup) -> MagicMock:
    mock = MagicMock()
    mock.__bool__ = lambda s: True
    return mock


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 9
    REQUIRED_KEYS = {
        "Constituição Estadual",
        "Emendas Constitucionais",
        "Lei Complementar",
        "Lei Delegada",
        "Lei Ordinária",
        "Decreto Legislativo",
        "Decreto",
        "Lei Promulgada",
        "Regimento Interno",
    }
    REQUIRE_INT_VALUES = False


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = LegislaAMScraper
    STATE_NAME = "Amazonas"

    def test_situations_is_empty_in_instance(self):
        scraper = _make_scraper()
        assert scraper.situations == {}


class TestFormatSearchUrl:
    def test_url_contains_type_id_year_page(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(41535, 2022, 3)
        assert "41535" in url
        assert "2022" in url
        assert "page=3" in url

    def test_url_starts_with_base_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(12, 2020, 1)
        assert url.startswith(scraper.base_url)

    def test_url_has_diario_am_path(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(10, 2021, 1)
        assert "/diario_am/" in url


class TestGetNormText:
    def test_short_text_returns_none(self):
        scraper = _make_scraper()
        html = (
            '<html><body><div class="materia rounded"><p>Curto</p></div></body></html>'
        )
        soup = BeautifulSoup(html, "lxml")
        result = scraper._get_norm_text(soup)
        assert result is None

    def test_valid_text_returns_beautifulsoup(self):
        scraper = _make_scraper()
        long_text = "A" * 200
        html = f'<html><body><div class="materia rounded"><p>{long_text}</p></div></body></html>'
        soup = BeautifulSoup(html, "lxml")
        result = scraper._get_norm_text(soup)
        assert result is not None
        assert hasattr(result, "find")

    def test_valid_text_wraps_in_html_structure(self):
        scraper = _make_scraper()
        long_text = "B" * 200
        html = f'<html><body><div class="materia rounded"><p>{long_text}</p></div></body></html>'
        soup = BeautifulSoup(html, "lxml")
        result = scraper._get_norm_text(soup)
        assert result.find("body") is not None
