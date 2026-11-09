"Parser and metadata regression tests for bahia_scraper."

from pathlib import Path
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from conftest import make_mock_response, make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.ba import SITUATIONS, TYPES, BahiaLegislaScraper

FIXTURES = Path(__file__).parents[2] / "fixtures" / "bahia"


def _html(name: str) -> str:
    return (FIXTURES / name).read_text()


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
        assert docs[0]["_html_link"] == "/doc/1"
        assert docs[0]["type"] == "Lei Ordinária"
        assert docs[1]["type"] == "Decreto"

    @pytest.mark.asyncio
    async def test_real_empty_result_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_html("listing_empty.html"), "lxml")
        assert await scraper._get_docs_links("http://example.com", soup=soup) == []
        assert scraper._get_total_pages(soup) == 1

    @pytest.mark.asyncio
    async def test_views_empty_without_message_raises(self):
        scraper = _make_scraper()
        html = _html("listing_empty.html").replace("Nenhum resultado encontrado", "")
        with pytest.raises(ValueError, match="No document rows"):
            await scraper._get_docs_links(
                "http://example.com", soup=BeautifulSoup(html, "lxml")
            )

    @pytest.mark.asyncio
    async def test_no_tbody_raises(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><div>nothing</div></body></html>", "lxml")
        scraper.http.request = AsyncMock(return_value=make_mock_response(str(soup)))
        with pytest.raises(ValueError, match="No results table"):
            await scraper._get_docs_links("http://example.com")

    @pytest.mark.asyncio
    async def test_rows_without_documents_raise(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            "<html><body><table><tbody><tr><td>only one</td></tr></tbody></table></body></html>",
            "lxml",
        )
        scraper.http.request = AsyncMock(return_value=make_mock_response(str(soup)))
        with pytest.raises(ValueError, match="No document rows"):
            await scraper._get_docs_links("http://example.com")

    @pytest.mark.asyncio
    async def test_passed_soup_skips_fetch(self):
        scraper = _make_scraper()
        soup = _make_listing_soup([("/doc/1", "Lei 001", "Leis Ordinárias")])
        docs = await scraper._get_docs_links("http://example.com", soup=soup)
        assert len(docs) == 1
        scraper.http.request.assert_not_called()

    @pytest.mark.asyncio
    async def test_real_listing_rows(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_html("listing_2012_p0.html"), "lxml")
        docs = await scraper._get_docs_links("http://example.com", soup=soup)
        assert docs[0] == {
            "title": "DECRETO SIMPLES DOE 29 e 30.12.2012",
            "type": "Decreto Simples",
            "_html_link": "/documentos/decreto-simples-doe-29-e-30122012",
        }
        assert len(docs) == 3


class TestPagination:
    def test_real_pager_gives_57_pages(self):
        soup = BeautifulSoup(_html("listing_2012_p0.html"), "lxml")
        assert _make_scraper()._get_total_pages(soup) == 57

    def test_header_total_without_pager_raises(self):
        soup = BeautifulSoup(_html("listing_2012_p0.html"), "lxml")
        soup.find("ul", class_="pagination").decompose()
        with pytest.raises(ValueError, match="no pager"):
            _make_scraper()._get_total_pages(soup)

    @pytest.mark.asyncio
    async def test_list_documents_fetches_every_page(self):
        scraper = _make_scraper()
        page = make_mock_response(_html("listing_2012_p0.html"))
        scraper.http.request = AsyncMock(return_value=page)
        refs = [ref async for ref in scraper.list_documents(2012)]
        pages = [
            parse_qs(urlsplit(call.args[0]).query)["page"]
            for call in scraper.http.request.await_args_list
        ]
        assert pages == [[str(p)] for p in range(57)]
        assert len(refs) == 57 * 3
        assert refs[0]["document_url"] == (
            "https://www.legislabahia.ba.gov.br/documentos/decreto-simples-doe-29-e-30122012"
        )


class TestFetch:
    @pytest.mark.asyncio
    async def test_real_document_metadata(self):
        scraper = _make_scraper()
        body = (FIXTURES / "document_lei_12616_2012.html").read_bytes()
        scraper.http.request = AsyncMock(return_value=make_mock_response(body))
        doc = await scraper.fetch(
            {
                "title": "LEI Nº 12.616 DE 28 DE DEZEMBRO DE 2012",
                "type": "Lei Ordinária",
                "year": 2012,
                "situation": "Não consta revogação expressa",
                "_html_link": "/documentos/lei-no-12616-de-28-de-dezembro-de-2012",
            }
        )
        assert doc.document_url == (
            "https://www.legislabahia.ba.gov.br/documentos/lei-no-12616-de-28-de-dezembro-de-2012"
        )
        assert doc.type == "Lei Ordinária"
        assert doc.norm_number == "12616"
        assert doc.date == "sexta-feira, 28 de Dezembro de 2012"
        assert doc.summary.startswith("Altera a Lei nº 11.371")
        assert "Art. 1º - Fica revogado o § 1º" in doc.text_markdown
        assert doc.summary not in doc.text_markdown
        assert doc.raw_ext == ".html"
        assert "_html_link" not in doc.model_dump()
