"Parser and metadata regression tests for amazonas_scraper."

from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup
from conftest import make_mock_response, make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.core.http import HttpError, Response
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


def _listing_response(href: str = "/diario_am/12/2012/4/2403") -> Response:
    html = f'<html><body><ul><li class="item-li"><h5>Lei nº 1</h5><p>Publicada em 04 de janeiro de 2012</p><a href="{href}">texto</a></li></ul></body></html>'
    return make_mock_response(html)


class TestListTypePagination:
    async def test_404_after_last_page_ends_listing(self):
        scraper = _make_scraper()
        scraper.http.request = AsyncMock(
            side_effect=[_listing_response(), HttpError("page2", 404, "Not Found")]
        )
        refs = [ref async for ref in scraper.list_type(2012, "Lei Ordinária", 12)]
        assert [ref["title"] for ref in refs] == ["Lei nº 1"]
        assert scraper.http.request.await_count == 2

    async def test_404_on_first_page_is_an_error(self):
        scraper = _make_scraper()
        scraper.http.request = AsyncMock(
            side_effect=HttpError("page1", 404, "Not Found")
        )
        with pytest.raises(HttpError):
            [ref async for ref in scraper.list_type(2012, "Lei Ordinária", 12)]


async def test_fetch_builds_document_from_http_body():
    scraper = _make_scraper()
    body = f'<html><body><div class="materia rounded"><p>{"Art. 1º Texto da lei. " * 5}</p></div></body></html>'.encode()
    scraper.http.request = AsyncMock(return_value=make_mock_response(body))
    ref = {
        "year": 2012,
        "type": "Lei Ordinária",
        "title": "Lei nº 1",
        "situation": "Não consta revogação expressa",
        "document_url": "https://legisla.imprensaoficial.am.gov.br/diario_am/12/2012/4/2403",
        "_html_link": "/diario_am/12/2012/4/2403",
    }
    doc = await scraper.fetch(ref)
    scraper.http.request.assert_awaited_once_with(ref["document_url"])
    assert "Art. 1º Texto da lei." in doc.text_markdown
    assert doc.raw == body and doc.raw_ext == ".html"
