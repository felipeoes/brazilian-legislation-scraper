from unittest.mock import AsyncMock, MagicMock

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import SituationsConstantTests, TypesConstantTests

from src.scrapers.states.ap import SITUATIONS, TYPES, AmapaAlapScraper


def _make_scraper(**kwargs) -> AmapaAlapScraper:
    return make_scraper(AmapaAlapScraper, **kwargs)


def _listing_soup(rows_html: str, total: int) -> BeautifulSoup:
    return BeautifulSoup(
        f"\n        <html>\n          <body>\n            <h1>BUSCAR LEGISLAÇÕES ({total})</h1>\n            <table>\n              <tbody>{rows_html}</tbody>\n            </table>\n          </body>\n        </html>\n        ",
        "lxml",
    )


def _doc_response(html: str) -> MagicMock:
    response = MagicMock()
    response.__bool__ = lambda self: True
    response.read = AsyncMock(return_value=html.encode("utf-8"))
    return response


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 5
    REQUIRED_KEYS = {
        "Decreto Legislativo",
        "Lei Complementar",
        "Lei Ordinária",
        "Resolução",
        "Emenda Constitucional",
    }
    REQUIRE_INT_VALUES = True


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_situations_contains_nao_consta(self):
        assert SITUATIONS["Não consta"] == "Não consta"


class TestFormatSearchUrl:
    def test_includes_year_and_page(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2020, 2)
        assert "especie_documento=" in url
        assert "ano=2020" in url
        assert "pagina=2" in url

    def test_supports_year_only_search(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2025, 1)
        assert "especie_documento=" in url
        assert "ano=2025" in url
