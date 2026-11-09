"""Regression tests for the Sergipe LegisOn API scraper, built from trimmed live responses."""

import json
from pathlib import Path
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest
from conftest import make_scraper
from parser_assertions import ScraperClassTests

from src.core.document import Document
from src.core.http import Response
from src.core.scraper import DEFAULT_VALID_SITUATION
from src.scrapers.states.se import (
    API_URL,
    INVALID_SITUATIONS,
    SITUATIONS,
    TYPES,
    VALID_SITUATIONS,
    SergipeLegsonScraper,
)

FIXTURES = Path(__file__).parents[2] / "fixtures" / "sergipe"
PDF_TEXT = "LEI COMPLEMENTAR Nº 222\n\nArt. 1º Fica alterado o § 4º do art. 17 da Lei Complementar nº 204."
FOOTER = (
    "\n\nEste texto não substitui o publicado no Diário Oficial do Estado\n\n5\n\n"
    "Extraído do Portal de Legislação do Governo de Sergipe - LegisOn https://legislacao.se.gov.br/\n"
)


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _response(payload: object, pagination: dict | None = None) -> Response:
    headers = {"Content-Type": "application/json"}
    if pagination is not None:
        headers["x-pagination"] = json.dumps(pagination)
    return Response(
        "https://api.legislacao.se.gov.br/api/v1/public/acts",
        200,
        headers,
        json.dumps(payload).encode(),
    )


def _listing_page(page: int, total_pages: int) -> Response:
    payload = _load("acts_lei_ordinaria_2012_p1.json")
    for act in payload["data"]:
        act["id"] += page * 1000
    return _response(payload, {"page": page, "totalPages": total_pages})


class TestConstants:
    def test_types_are_api_ids(self):
        assert TYPES == {
            "Lei Ordinária": 3,
            "Lei Complementar": 2,
            "Decreto": 4,
            "Emenda Constitucional": 8,
        }

    def test_situations(self):
        assert VALID_SITUATIONS == {1: "Em Vigor"}
        assert INVALID_SITUATIONS == {2: "Revogado", 3: "Declarado Inconstitucional"}
        assert SITUATIONS == VALID_SITUATIONS | INVALID_SITUATIONS


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = SergipeLegsonScraper
    STATE_NAME = "Sergipe"


class TestListType:
    async def test_paginates_and_builds_refs(self):
        scraper = make_scraper(SergipeLegsonScraper)

        async def request(url: str) -> Response:
            return _listing_page(int(parse_qs(urlsplit(url).query)["Page"][0]), 3)

        scraper.http.request = AsyncMock(side_effect=request)
        refs = [ref async for ref in scraper.list_type(2012, "Lei Ordinária", 3)]

        queries = [
            parse_qs(urlsplit(c.args[0]).query)
            for c in scraper.http.request.await_args_list
        ]
        assert sorted(q["Page"][0] for q in queries) == ["1", "2", "3"]
        assert all(
            q["Year"] == ["2012"] and q["TypeAct"] == ["3"] and q["Quantity"] == ["50"]
            for q in queries
        )
        assert len(refs) == 6
        assert refs[0] == {
            "year": 2012,
            "type": "Lei Ordinária",
            "title": "Lei Ordinária 7495 de 2012",
            "summary": "Reconhece de Utilidade Pública Estadual a ASSOCIAÇÃO COMUNITÁRIA BOM JESUS DOS NAVEGANTES, com sede e foro na Cidade de Barra dos Coqueiros - SE.",
            "number": "7495",
            "date": "2012-12-18",
            "publication_date": "2012-12-28",
            "document_url": f"{API_URL}/9364/act",
        }

    async def test_empty_type_yields_nothing(self):
        scraper = make_scraper(SergipeLegsonScraper)
        payload = {"success": True, "statusCode": 200, "data": []}
        scraper.http.request = AsyncMock(
            return_value=_response(payload, {"totalItens": 0, "totalPages": 0})
        )
        assert [
            ref async for ref in scraper.list_type(2012, "Emenda Constitucional", 8)
        ] == []
        assert scraper.http.request.await_count == 1

    @pytest.mark.parametrize(
        ("payload", "pagination"),
        [
            ({"success": True, "data": []}, None),
            ({"success": False, "statusCode": 500, "data": None}, {"totalPages": 0}),
            ({"success": True, "data": {"id": 1}}, {"totalPages": 1}),
            ([], {"totalPages": 1}),
        ],
    )
    async def test_malformed_listing_raises(self, payload, pagination):
        scraper = make_scraper(SergipeLegsonScraper)
        scraper.http.request = AsyncMock(return_value=_response(payload, pagination))
        with pytest.raises(ValueError, match="Malformed Sergipe listing"):
            [ref async for ref in scraper.list_type(2012, "Lei Ordinária", 3)]


def _ref(act_id: int) -> dict:
    return {
        "year": 2012,
        "type": "Lei Complementar",
        "title": "Lei Complementar 222 de 2012",
        "summary": "listing summary",
        "number": "222",
        "date": "2012-05-31",
        "publication_date": "2012-06-01",
        "document_url": f"{API_URL}/{act_id}/act",
    }


class TestFetch:
    async def test_prefers_consolidated_pdf_and_keeps_stable_url(self):
        act = _load("act_263.json")
        scraper = make_scraper(
            SergipeLegsonScraper,
            download=AsyncMock(return_value=(PDF_TEXT + FOOTER, b"%PDF-1.4", ".pdf")),
        )
        scraper.http.request = AsyncMock(return_value=_response(act))

        doc = await scraper.fetch(_ref(263))

        scraper.http.request.assert_awaited_once_with(f"{API_URL}/263/act")
        scraper.download.assert_awaited_once_with(
            act["data"]["arquivoAtualizadoPdf"]["caminho"]
        )
        assert isinstance(doc, Document)
        assert doc.document_url == f"{API_URL}/263/act"
        assert doc.situation == "Em Vigor"
        assert doc.summary.startswith(
            "Altera o § 4º do art. 17 da Lei Complementar nº 204"
        )
        assert doc.text_markdown == PDF_TEXT
        assert (doc.raw, doc.raw_ext) == (b"%PDF-1.4", ".pdf")
        assert doc.model_dump()["number"] == "222"

    async def test_falls_back_to_original_pdf(self):
        act = _load("act_460.json")
        act["data"]["idSituacao"] = None
        scraper = make_scraper(
            SergipeLegsonScraper,
            download=AsyncMock(return_value=(PDF_TEXT, b"%PDF-1.3", ".pdf")),
        )
        scraper.http.request = AsyncMock(return_value=_response(act))

        doc = await scraper.fetch(_ref(460))

        scraper.download.assert_awaited_once_with(act["data"]["arquivo"]["caminho"])
        assert doc.situation == DEFAULT_VALID_SITUATION

    async def test_revoked_situation(self):
        act = _load("act_460.json")
        act["data"]["idSituacao"] = 2
        scraper = make_scraper(
            SergipeLegsonScraper,
            download=AsyncMock(return_value=(PDF_TEXT, b"%PDF-1.3", ".pdf")),
        )
        scraper.http.request = AsyncMock(return_value=_response(act))
        assert (await scraper.fetch(_ref(460))).situation == "Revogado"

    async def test_act_without_file_raises(self):
        act = _load("act_460.json")
        act["data"]["arquivo"] = None
        scraper = make_scraper(SergipeLegsonScraper)
        scraper.http.request = AsyncMock(return_value=_response(act))
        with pytest.raises(ValueError, match="no PDF"):
            await scraper.fetch(_ref(460))


class TestCleanLegisonMarkdown:
    def test_removes_portal_footer_and_disclaimer(self):
        scraper = make_scraper(SergipeLegsonScraper)
        result = scraper._clean_legison_markdown(PDF_TEXT + FOOTER)
        assert "LegisOn" not in result and "não substitui" not in result
        assert result == PDF_TEXT

    @pytest.mark.parametrize(
        "footer",
        [
            "Extraído do Portal de Legislação do Governo de Sergipe - LegisOn https://legislacao.se.gov.b",
            "Extraído do Portal de Legislação do Governo de Sergipe - LegisOn ",
        ],
    )
    def test_removes_truncated_portal_footer(self, footer):
        scraper = make_scraper(SergipeLegsonScraper)
        assert (
            scraper._clean_legison_markdown(f"{PDF_TEXT}\n\n{footer}\n\nArt. 4º")
            == f"{PDF_TEXT}\n\nArt. 4º"
        )

    def test_collapses_multiple_newlines(self):
        scraper = make_scraper(SergipeLegsonScraper)
        assert (
            scraper._clean_legison_markdown("Linha 1.\n\n\n\n\nLinha 2.")
            == "Linha 1.\n\nLinha 2."
        )


@pytest.mark.integration
async def test_live_listing_and_fetch(integration_scraper_factory):
    async with integration_scraper_factory(SergipeLegsonScraper, year=2012) as scraper:
        refs = [ref async for ref in scraper.list_type(2012, "Lei Complementar", 2)]
        assert len(refs) == 8
        doc = await scraper.fetch(refs[0])
        assert isinstance(doc, Document) and doc.raw_ext == ".pdf"
        assert doc.document_url == refs[0]["document_url"]
