"""Regression tests for the Pará Banco de Leis scraper, built from trimmed live pages."""

from pathlib import Path
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import fitz
import pytest
from bs4 import BeautifulSoup
from conftest import make_mock_response, make_scraper
from parser_assertions import ScraperClassTests, TypesConstantTests

from src.core.scraper import DEFAULT_VALID_SITUATION
from src.scrapers.states.pa import TYPES, ParaAlepaScraper

FIXTURES = Path(__file__).parents[2] / "fixtures" / "para"


def _html(name: str) -> str:
    return (FIXTURES / name).read_text()


def _pdf(text: str) -> bytes:
    doc = fitz.open()
    doc.new_page().insert_textbox(fitz.Rect(72, 72, 520, 770), text, fontsize=12)
    content = doc.tobytes()
    doc.close()
    return content


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 6
    REQUIRED_KEYS = {"Decreto Estadual", "Lei Ordinária", "Lei Complementar"}
    REQUIRE_INT_VALUES = True

    def test_federal_amendments_excluded(self):
        assert 12 not in TYPES.values()


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = ParaAlepaScraper
    STATE_NAME = "Pará"


class TestParseRows:
    def test_parses_real_listing_rows(self):
        scraper = make_scraper(ParaAlepaScraper)
        soup = BeautifulSoup(_html("listing_2012_decreto_estadual_p1.html"), "lxml")
        rows = scraper._parse_rows(soup)
        assert [row["norm_number"] for row in rows] == ["640", "639", "638"]
        assert rows[0]["year"] == 2012
        assert rows[0]["document_url"] == (
            "https://bancodeleis.alepa.pa.gov.br/arquivos/lei640_2012_42751.pdf"
        )
        assert rows[0]["summary"].startswith("D E C R E T O Nº 640")
        assert "DOE Nº 32.307" in rows[0]["summary"]

    def test_total_pages_from_pagination(self):
        soup = BeautifulSoup(_html("listing_2012_decreto_estadual_p1.html"), "lxml")
        assert ParaAlepaScraper._total_pages(soup) == 20

    def test_results_without_pager_raise(self):
        soup = BeautifulSoup(_html("listing_2012_decreto_estadual_p1.html"), "lxml")
        soup.find(id="paginacao").decompose()
        with pytest.raises(ValueError, match="no pager"):
            ParaAlepaScraper._total_pages(soup)

    def test_explicit_empty_result(self):
        scraper = make_scraper(ParaAlepaScraper)
        soup = BeautifulSoup(_html("listing_empty.html"), "lxml")
        assert scraper._parse_rows(soup) == []
        assert ParaAlepaScraper._total_pages(soup) == 1

    def test_zero_count_without_empty_message_raises(self):
        scraper = make_scraper(ParaAlepaScraper)
        html = _html("listing_empty.html").replace("Nenhuma lei encontrada", "Erro")
        with pytest.raises(ValueError, match="empty message"):
            scraper._parse_rows(BeautifulSoup(html, "lxml"))

    def test_missing_results_table_raises(self):
        scraper = make_scraper(ParaAlepaScraper)
        soup = BeautifulSoup("<html><body><p>Manutenção</p></body></html>", "lxml")
        with pytest.raises(ValueError, match="result count"):
            scraper._parse_rows(soup)

    def test_row_without_pdf_raises(self):
        scraper = make_scraper(ParaAlepaScraper)
        html = _html("listing_2012_decreto_estadual_p1.html").replace(
            "btn-pdf-t", "btn-x"
        )
        with pytest.raises(ValueError, match="Malformed listing row"):
            scraper._parse_rows(BeautifulSoup(html, "lxml"))


class TestListType:
    async def test_fetches_every_page_for_year_and_type(self):
        scraper = make_scraper(ParaAlepaScraper)
        page = _html("listing_2012_decreto_estadual_p1.html")
        scraper.http.request = AsyncMock(return_value=make_mock_response(page))
        refs = [ref async for ref in scraper.list_type(2012, "Decreto Estadual", 2)]
        queries = [
            parse_qs(urlsplit(call.args[0]).query)
            for call in scraper.http.request.await_args_list
        ]
        assert [q["p"] for q in queries] == [[str(p)] for p in range(1, 21)]
        assert all(q["ano"] == ["2012"] and q["tipo"] == ["2"] for q in queries)
        assert len(refs) == 60
        assert refs[0]["title"] == "Decreto Estadual nº 640/2012"
        assert refs[0]["type"] == "Decreto Estadual"
        assert refs[0]["situation"] == DEFAULT_VALID_SITUATION

    async def test_malformed_first_page_raises(self):
        scraper = make_scraper(ParaAlepaScraper)
        scraper.http.request = AsyncMock(
            return_value=make_mock_response("<html><body></body></html>")
        )
        with pytest.raises(ValueError):
            [ref async for ref in scraper.list_type(2012, "Lei Ordinária", 3)]


class TestFetch:
    async def test_fetch_keeps_listing_metadata(self):
        scraper = make_scraper(ParaAlepaScraper)
        url = "https://bancodeleis.alepa.pa.gov.br/arquivos/lei640_2012_42751.pdf"
        text = "DECRETO Nº 640, DE 26 DE DEZEMBRO DE 2012. Art. 1º Fica concedido desconto pela antecipação do pagamento do IPVA."
        scraper.http.request = AsyncMock(
            return_value=make_mock_response(_pdf(text), content_type="application/pdf")
        )
        doc = await scraper.fetch(
            {
                "title": "Decreto Estadual nº 640/2012",
                "type": "Decreto Estadual",
                "year": 2012,
                "norm_number": "640",
                "summary": "D E C R E T O Nº 640",
                "situation": DEFAULT_VALID_SITUATION,
                "document_url": url,
            }
        )
        scraper.http.request.assert_awaited_once_with(url)
        assert doc.document_url == url
        assert doc.raw_ext == ".pdf"
        assert doc.norm_number == "640"
        assert "Art. 1º" in doc.text_markdown


@pytest.mark.integration
async def test_live_listing_2012_lei_complementar():
    from src.core.http import HttpClient
    from src.core.store import Store

    async with ParaAlepaScraper(
        store=Store(":memory:"), http=HttpClient(rps=2), years=range(2012, 2013)
    ) as scraper:
        refs = [ref async for ref in scraper.list_type(2012, "Lei Complementar", 4)]
    assert len(refs) == 6
    assert all(ref["document_url"].endswith(".pdf") for ref in refs)
