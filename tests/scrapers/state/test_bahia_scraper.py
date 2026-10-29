"Parser and metadata regression tests for bahia_scraper."

from unittest.mock import AsyncMock

import pytest
from bs4 import BeautifulSoup
from conftest import make_mock_response, make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.ba import SITUATIONS, TYPES, BahiaLegislaScraper


def _make_scraper(**kwargs) -> BahiaLegislaScraper:
    """Instantiate BahiaLegislaScraper with injected dependencies."""
    return make_scraper(BahiaLegislaScraper, **kwargs)


def _make_listing_soup(
    rows: list[tuple[str, str, str]], *, last_page_index: int | None = None
) -> BeautifulSoup:
    """Build a Bahia listing table from (href, title, category) rows."""
    trs = ""
    for href, title, category in rows:
        trs += f'<tr><td><b>{title}</b><a href="{href}">{title}</a></td><td>{category}</td></tr>'
    pagination = ""
    if last_page_index is not None:
        pagination = f'<ul class="pagination js-pager__items"><li class="pager__item pager__item--last"><a href="?page={last_page_index}">Ultimo</a></li></ul>'
    return BeautifulSoup(
        f"<html><body><table><tbody>{trs}</tbody></table>{pagination}</body></html>",
        "lxml",
    )


def _make_doc_soup(
    *,
    category: str = "Leis Ordinárias",
    body_html: str = "<p>" + "Texto da lei. " * 30 + "</p>",
    revogado_span: bool = False,
    revogado_div_text: str = "",
) -> bytes:
    """Build a minimal document page for Bahia (returns bytes)."""
    revogado_span_html = (
        '<span class="revogado">revogado</span>' if revogado_span else ""
    )
    revogado_div_html = (
        f'<div class="alteracao">{revogado_div_text}</div>' if revogado_div_text else ""
    )
    return f'<html><body>\n        <div class="field--name-field-categoria-doc"><div class="field--item">{category}</div></div>\n        <div class="field--name-field-numero-doc"><div class="field--item">001</div></div>\n        <div class="field--name-field-data-doc"><div class="field--item">2022-01-01</div></div>\n        <div class="field--name-field-data-de-publicacao-no-doe"><div class="field--item">2022-01-05</div></div>\n        <div class="field--name-field-ementa"><div class="field--item">Ementa da lei.</div></div>\n        <div class="field--name-body">\n            {revogado_span_html}\n            {revogado_div_html}\n            {body_html}\n        </div>\n    </body></html>'.encode()


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 16
    REQUIRED_KEYS = {"Lei Complementar", "Constituição Estadual Atual 1989"}
    REQUIRE_INT_VALUES = True


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = BahiaLegislaScraper
    STATE_NAME = "Bahia"

    def test_situations_is_empty_in_instance(self):
        scraper = _make_scraper()
        assert scraper.situations == {}

    def test_revogado_regex_set(self):
        assert hasattr(BahiaLegislaScraper, "_REVOGADO_RE")


class TestNormalizeType:
    def test_maps_plural_site_label(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Leis Ordinárias") == "Lei Ordinária"

    def test_maps_decretos_numerados_to_decreto(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Decretos Numerados") == "Decreto"

    def test_unknown_type_returns_cleaned_label(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("  Tipo   Especial  ") == "Tipo Especial"


class TestBuildSearchUrl:
    def test_url_contains_categoria_when_provided(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(7, 2022, 0)
        assert "categoria" in url
        assert "7" in url

    def test_url_omits_categoria_for_year_only_query(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(None, 2022, 0)
        assert "categoria" not in url

    def test_url_contains_year_bounds_and_page(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(None, 2022, 3)
        assert "2022-01-01" in url
        assert "2022-12-31" in url
        assert "page=3" in url
        assert url.startswith(scraper.base_url)


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_happy_path_returns_docs_with_normalized_type(self):
        scraper = _make_scraper()
        soup = _make_listing_soup(
            [
                ("/doc/1", "Lei Ordinária 001/2022", "Leis Ordinárias"),
                ("/doc/2", "Decreto 002/2022", "Decretos Numerados"),
            ]
        )
        scraper.http.request = AsyncMock(return_value=make_mock_response(str(soup)))
        docs = await scraper._get_docs_links("http://example.com")
        assert len(docs) == 2
        assert docs[0]["title"] == "Lei Ordinária 001/2022"
        assert docs[0]["html_link"] == "/doc/1"
        assert docs[0]["type"] == "Lei Ordinária"
        assert docs[1]["type"] == "Decreto"

    @pytest.mark.asyncio
    async def test_empty_page_class_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            '<html><body><td class="views-empty">Nenhum resultado encontrado</td></body></html>',
            "lxml",
        )
        scraper.http.request = AsyncMock(return_value=make_mock_response(str(soup)))
        docs = await scraper._get_docs_links("http://example.com")
        assert docs == []

    @pytest.mark.asyncio
    async def test_no_tbody_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><div>nothing</div></body></html>", "lxml")
        scraper.http.request = AsyncMock(return_value=make_mock_response(str(soup)))
        docs = await scraper._get_docs_links("http://example.com")
        assert docs == []

    @pytest.mark.asyncio
    async def test_row_with_wrong_column_count_is_skipped(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            "<html><body><table><tbody><tr><td>only one</td></tr></tbody></table></body></html>",
            "lxml",
        )
        scraper.http.request = AsyncMock(return_value=make_mock_response(str(soup)))
        docs = await scraper._get_docs_links("http://example.com")
        assert docs == []

    @pytest.mark.asyncio
    async def test_passed_soup_skips_fetch(self):
        scraper = _make_scraper()
        scraper._fetch_soup_with_retry = AsyncMock()
        soup = _make_listing_soup([("/doc/1", "Lei 001", "Leis Ordinárias")])
        docs = await scraper._get_docs_links("http://example.com", soup=soup)
        assert len(docs) == 1
        scraper._fetch_soup_with_retry.assert_not_called()
