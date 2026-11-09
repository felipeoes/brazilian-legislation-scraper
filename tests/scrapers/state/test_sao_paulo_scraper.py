"""Regression tests for the São Paulo (ALESP) API scraper, built from captured responses."""

import json
from pathlib import Path
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest
from conftest import make_mock_json_response, make_mock_response, make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.sp import (
    INVALID_SITUATIONS,
    SITUATIONS,
    TYPES,
    VALID_SITUATIONS,
    SaoPauloAlespScraper,
)

FIXTURES = Path(__file__).parents[2] / "fixtures" / "sao_paulo"
LISTING = json.loads(
    (FIXTURES / "normas_2012_situacao1_page0.json").read_text(encoding="utf-8")
)


def _empty_listing() -> dict:
    return {**LISTING, "content": [], "totalElements": 0, "totalPages": 0}


def _query(url: str) -> dict[str, str]:
    return {key: values[0] for key, values in parse_qs(urlsplit(url).query).items()}


async def _refs(scraper: SaoPauloAlespScraper, year: int = 2012) -> list[dict]:
    return [ref async for ref in scraper.list_documents(year)]


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 14
    REQUIRED_KEYS = {"Decreto", "Lei", "Lei Complementar", "Constituição Estadual"}
    REQUIRE_INT_VALUES = True


class TestSituationsConstants(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_situations_is_union(self):
        assert SITUATIONS == {**VALID_SITUATIONS, **INVALID_SITUATIONS}
        assert VALID_SITUATIONS == {"Sem revogação expressa": 1}
        assert INVALID_SITUATIONS["Revogada"] == 5


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = SaoPauloAlespScraper
    STATE_NAME = "São Paulo"


class TestListing:
    async def test_lists_each_situation_and_every_page(self):
        def respond(url: str) -> object:
            query = _query(url)
            if query["idsSituacao"] != "1":
                return make_mock_json_response(_empty_listing())
            return make_mock_json_response(
                LISTING if query["page"] == "0" else {**LISTING, "content": []}
            )

        scraper = make_scraper(SaoPauloAlespScraper)
        scraper.http.request = AsyncMock(side_effect=respond)
        refs = await _refs(scraper)

        queries = [
            _query(call.args[0]) for call in scraper.http.request.await_args_list
        ]
        # 6 situations; situation 1 has 1528 norms → 4 pages of 500.
        assert len(queries) == 6 + 3
        assert {q["idsSituacao"] for q in queries} == {str(i) for i in range(1, 7)}
        assert sorted(q["page"] for q in queries if q["idsSituacao"] == "1") == [
            "0",
            "1",
            "2",
            "3",
        ]
        assert all(q["ano"] == "2012" and q["size"] == "500" for q in queries)
        assert len(refs) == 4

    async def test_ref_carries_listing_metadata(self):
        def respond(url: str) -> object:
            query = _query(url)
            if query["idsSituacao"] != "5":
                return make_mock_json_response(_empty_listing())
            return make_mock_json_response({**LISTING, "totalElements": 4})

        scraper = make_scraper(SaoPauloAlespScraper)
        scraper.http.request = AsyncMock(side_effect=respond)
        lei, _ato, decreto, no_text = await _refs(scraper)

        assert lei["year"] == 2012
        assert lei["title"] == "Lei nº 14.924, de 28/12/2012"
        assert lei["type"] == "Lei"
        assert lei["situation"] == "Revogada"
        assert lei["summary"].startswith("Dispõe sobre o subsídio do Governador")
        assert lei["number"] == "14924"
        assert lei["date"] == "28/12/2012"
        assert lei["document_url"] == "https://www.al.sp.gov.br/norma/169098"
        assert lei["text_url"].endswith("/lei/2012/lei-14924-28.12.2012.html")
        assert decreto["type"] == "Decreto"
        assert decreto["text_url"].endswith(".pdf")
        assert no_text["text_url"] is None

    async def test_malformed_listing_raises(self):
        scraper = make_scraper(SaoPauloAlespScraper)
        for body in ({}, {"content": None, "totalElements": 1}, []):
            scraper.http.request = AsyncMock(return_value=make_mock_json_response(body))
            with pytest.raises(ValueError, match="Malformed"):
                [ref async for ref in scraper._list_situation(2012, "Revogada", 5)]

    async def test_html_listing_response_raises(self):
        scraper = make_scraper(SaoPauloAlespScraper)
        scraper.http.request = AsyncMock(
            return_value=make_mock_response("<html>erro</html>")
        )
        with pytest.raises(json.JSONDecodeError):
            [ref async for ref in scraper._list_situation(2012, "Revogada", 5)]

    async def test_failed_situation_is_recorded_and_others_continue(self):
        def respond(url: str) -> object:
            if _query(url)["idsSituacao"] == "1":
                return make_mock_json_response({})
            return make_mock_json_response({**LISTING, "totalElements": 4})

        scraper = make_scraper(SaoPauloAlespScraper)
        scraper.http.request = AsyncMock(side_effect=respond)
        scraper.error = AsyncMock()
        refs = await _refs(scraper)

        assert len(refs) == 4 * 5
        scraper.error.assert_awaited_once()
        assert scraper.error.await_args.args[0]["situation"] == "Sem revogação expressa"


def _ref(**overrides: object) -> dict:
    return {
        "year": 2012,
        "type": "Ato do Presidente",
        "title": "Ato do Presidente nº 83, de 20/12/2012",
        "summary": "Convoca suplente para tomar posse como Deputado titular",
        "situation": "Sem revogação expressa",
        "document_url": "https://www.al.sp.gov.br/norma/169110",
        "text_url": "https://www.al.sp.gov.br/repositorio/legislacao/ato.do.presidente/2012/ato.do.presidente-83-20.12.2012.html",
        **overrides,
    }


class TestFetch:
    async def test_html_decodes_meta_charset_and_strips_site_header(self):
        body = (FIXTURES / "ato.do.presidente-83-20.12.2012.html").read_bytes()
        scraper = make_scraper(SaoPauloAlespScraper)
        scraper.http.request = AsyncMock(return_value=make_mock_response(body))
        doc = await scraper.fetch(_ref())

        scraper.http.request.assert_awaited_once_with(_ref()["text_url"])
        assert "DO ESTADO DE SÃO PAULO" in doc.text_markdown
        assert "ORLANDO JOSÉ BOLÇONE" in doc.text_markdown
        assert "LogoDECO" not in doc.text_markdown
        assert doc.document_url == "https://www.al.sp.gov.br/norma/169110"
        assert doc.text_url == _ref()["text_url"]
        assert (doc.type, doc.situation) == (
            "Ato do Presidente",
            "Sem revogação expressa",
        )
        assert doc.raw == body
        assert doc.raw_ext == ".html"

    async def test_html_keeps_links_to_other_norms(self):
        body = (FIXTURES / "lei-14924-28.12.2012.html").read_bytes()
        scraper = make_scraper(SaoPauloAlespScraper)
        scraper.http.request = AsyncMock(return_value=make_mock_response(body))
        doc = await scraper.fetch(
            _ref(type="Lei", text_url="https://example.org/lei-14924.html")
        )

        assert "Ficha informativa" not in doc.text_markdown
        assert "Assembleia Legislativa do Estado de São Paulo" not in doc.text_markdown
        assert doc.text_markdown.startswith("# LEI Nº 14.924")  # no <head> front matter
        assert "Lei complementar nº 802, de 7 de dezembro de 1995" in doc.text_markdown

    async def test_pdf_text_is_downloaded(self):
        pdf_url = "https://www.al.sp.gov.br/repositorio/legislacao/decreto/2012/decreto-58791-21.12.2012.pdf"
        markdown = "Texto do decreto convertido do PDF. " * 5
        scraper = make_scraper(SaoPauloAlespScraper)
        scraper.download = AsyncMock(return_value=(markdown, b"%PDF-1.5", ".pdf"))
        doc = await scraper.fetch(_ref(type="Decreto", text_url=pdf_url))

        scraper.download.assert_awaited_once_with(pdf_url)
        assert doc.text_markdown == markdown.strip()
        assert (doc.raw, doc.raw_ext) == (b"%PDF-1.5", ".pdf")
        assert doc.document_url == "https://www.al.sp.gov.br/norma/169110"

    async def test_norm_without_text_fails_visibly(self):
        scraper = make_scraper(SaoPauloAlespScraper)
        with pytest.raises(ValueError, match="no text"):
            await scraper.fetch(_ref(text_url=None))


@pytest.mark.integration
async def test_live_2012_listing_matches_type_counts():
    from src.core.http import HttpClient
    from src.core.store import Store

    async with SaoPauloAlespScraper(
        store=Store(":memory:"), http=HttpClient(rps=2), years=range(2012, 2013)
    ) as scraper:
        refs = await _refs(scraper)
        doc = await scraper.fetch(
            next(r for r in refs if r["type"] == "Lei Complementar")
        )
    counts = {t: sum(r["type"] == t for r in refs) for t in ("Lei", "Decreto")}
    assert len(refs) == 1656
    assert counts == {"Lei": 239, "Decreto": 1103}
    assert doc.raw_ext == ".html"
