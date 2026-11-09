from pathlib import Path
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest
from conftest import make_mock_response, make_scraper
from parser_assertions import TypesConstantTests

from src.core.scraper import DEFAULT_VALID_SITUATION
from src.scrapers.states.ap import TYPES, AmapaAlapScraper

FIXTURES = Path(__file__).parents[2] / "fixtures" / "amapa"
PORTAL = "https://elegis.al.ap.leg.br/portal"


def _html(name: str, **replace: str) -> object:
    body = (FIXTURES / name).read_text(encoding="utf-8")
    for old, new in replace.items():
        body = body.replace(old, new)
    return make_mock_response(body, content_type="text/html; charset=utf-8")


def _query(call: object) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(call.args[0]).query).items()}


async def _list(scraper: AmapaAlapScraper, year: int = 2012) -> list[dict]:
    return [ref async for ref in scraper.list_type(year, "Lei Ordinária", 1)]


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


async def test_listing_rows_parse_metadata_and_text_links():
    scraper = make_scraper(AmapaAlapScraper)
    listing = _html("listing.html", **{"<strong>131</strong>": "<strong>4</strong>"})
    scraper.http.request = AsyncMock(return_value=listing)
    refs = await _list(scraper)
    scraper.http.request.assert_awaited_once()
    assert _query(scraper.http.request.call_args) == {
        "ano": "2012",
        "tipo_proposicao_id": "1",
        "page": "1",
    }
    assert refs[0] == {
        "year": 2015,
        "type": "Lei Ordinária",
        "situation": DEFAULT_VALID_SITUATION,
        "title": "Lei Ordinária nº 1890, de 06/05/15",
        "summary": "Autoriza a criação do Centro de Atendimento a Crianças e Jovens Dependentes de Substâncias Químicas.",
        "doe_number": "5949",
        "publication_date": "06/05/2015",
        "proposition": "Projeto de Lei Ordinária nº 0122/12-AL",
        "document_url": f"{PORTAL}/proposicao/21649",
        "_html_link": f"{PORTAL}/legislacao/1660/imprimir",
    }
    # No publication date: the year comes from the norm date in the title.
    assert (refs[1]["year"], refs[1]["publication_date"]) == (2015, "")
    # Only the full text: fall back to it.
    assert refs[2]["year"] == 2012
    assert refs[2]["_html_link"] == f"{PORTAL}/proposicao/22607/texto-integral/imprimir"
    # No text link at all (recent norms): listed, fetch fails visibly.
    assert refs[3]["_html_link"] is None
    assert refs[3]["document_url"] == f"{PORTAL}/proposicao/102832"


async def test_listing_fetches_remaining_pages_from_counter():
    scraper = make_scraper(AmapaAlapScraper)
    scraper.http.request = AsyncMock(return_value=_html("listing.html"))
    refs = await _list(scraper)
    pages = sorted(int(_query(c)["page"]) for c in scraper.http.request.call_args_list)
    assert pages == [1, 2, 3, 4, 5, 6, 7]  # 131 items, 20 per page
    assert len(refs) == 4 * 7


async def test_empty_listing_yields_nothing():
    scraper = make_scraper(AmapaAlapScraper)
    scraper.http.request = AsyncMock(return_value=_html("listing_empty.html"))
    assert await _list(scraper, 1991) == []
    scraper.http.request.assert_awaited_once()


@pytest.mark.parametrize(
    ("body", "match"),
    [
        ("<html><body><p>Erro</p></body></html>", "counter"),
        (
            (FIXTURES / "listing_empty.html")
            .read_text(encoding="utf-8")
            .replace("<strong>0</strong>", "<strong>3</strong>"),
            "no rows",
        ),
    ],
)
async def test_malformed_listing_raises(body, match):
    scraper = make_scraper(AmapaAlapScraper)
    scraper.http.request = AsyncMock(return_value=make_mock_response(body))
    with pytest.raises(ValueError, match=match):
        await _list(scraper)


async def test_fetch_uses_printable_text_and_keeps_listing_metadata():
    scraper = make_scraper(AmapaAlapScraper)
    scraper.http.request = AsyncMock(return_value=_html("listing.html"))
    ref = (await _list(scraper))[0]
    scraper.http.request = AsyncMock(return_value=_html("consolidado_imprimir.html"))
    doc = await scraper.fetch(dict(ref))
    scraper.http.request.assert_awaited_once_with(f"{PORTAL}/legislacao/1660/imprimir")
    assert doc.document_url == f"{PORTAL}/legislacao/1660"
    assert (doc.year, doc.type, doc.title) == (2015, ref["type"], ref["title"])
    assert doc.situation == DEFAULT_VALID_SITUATION
    assert doc.summary == ref["summary"]
    assert "LEI Nº 1.890, DE 06 DE MAIO DE 2015" in doc.text_markdown
    assert "Art. 3º" in doc.text_markdown
    assert "print" not in doc.text_markdown
    assert doc.raw_ext == ".html" and doc.raw.startswith(b"<p")


async def test_fetch_without_text_link_fails_without_requests():
    scraper = make_scraper(AmapaAlapScraper)
    scraper.http.request = AsyncMock(return_value=_html("listing.html"))
    ref = (await _list(scraper))[3]
    scraper.http.request.reset_mock()
    with pytest.raises(ValueError, match="no text"):
        await scraper.fetch(dict(ref))
    scraper.http.request.assert_not_awaited()


@pytest.mark.integration
async def test_live_2012_complementary_laws(integration_scraper_factory):
    async with integration_scraper_factory(AmapaAlapScraper, year=2012) as scraper:
        refs = [ref async for ref in scraper.list_type(2012, "Lei Complementar", 2)]
        assert len(refs) >= 9
        doc = await scraper.fetch(next(r for r in refs if r["_html_link"]))
        assert "Art" in doc.text_markdown
