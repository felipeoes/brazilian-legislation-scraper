"""Tests for ParaibaAlpbScraper.

Covers:
- TYPES constant: 11 types present, integer IDs
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set (SAPLBaseScraper passes situations={})
- _id_to_type reverse map built correctly
- _get_docs_links includes tipo_id field

Run with:
    .venv/bin/pytest tests/test_paraiba_scraper.py -v
"""

import pytest
from conftest import make_scraper
from parser_assertions import ScraperClassTests, TypesConstantTests

from src.scrapers.states.pb import TYPES, ParaibaAlpbScraper


def _make_scraper(**kwargs) -> ParaibaAlpbScraper:
    """Instantiate ParaibaAlpbScraper with injected dependencies."""
    kwargs.setdefault("subjects", {})
    kwargs.setdefault("_id_to_type", {v: k for k, v in TYPES.items()})
    kwargs.setdefault("_page_size", 100)
    return make_scraper(ParaibaAlpbScraper, **kwargs)


def _make_api_item(
    doc_id: int = 1,
    texto_integral: str | None = "https://sapl3.al.pb.leg.br/media/lei.pdf",
    tipo: int = 2,
    **kwargs,
) -> dict:
    return {
        "id": doc_id,
        "numero": str(doc_id),
        "__str__": f"Lei Ordinária {doc_id}/2023",
        "link_detail_backend": f"/norma/{doc_id}",
        "ementa": "Ementa da lei.",
        "assuntos": [],
        "data": "2023-01-01",
        "ano": 2023,
        "esfera_federacao": None,
        "veiculo_publicacao": None,
        "texto_integral": texto_integral,
        "data_vigencia": None,
        "tipo": tipo,
        **kwargs,
    }


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 11
    REQUIRED_KEYS = {"Lei Ordinária", "Lei Complementar", "Constituição Estadual"}


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = ParaibaAlpbScraper
    STATE_NAME = "Paraíba"

    def test_situations_empty(self):
        scraper = _make_scraper()
        assert scraper.situations == {}

    def test_id_to_type_built_correctly(self):
        scraper = _make_scraper()
        assert scraper._id_to_type[2] == "Lei Ordinária"
        assert scraper._id_to_type[1] == "Lei Complementar"
        assert len(scraper._id_to_type) == len(TYPES)


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_tipo_id_present_in_result(self):
        scraper = _make_scraper()
        item = _make_api_item(doc_id=1, tipo=2)
        data = {"results": [item]}
        result = await scraper._get_docs_links("https://example.com/api/", data=data)
        assert len(result) == 1
        assert result[0]["type"] == scraper._resolve_type_name(2)

    @pytest.mark.asyncio
    async def test_tipo_id_matches_api_tipo(self):
        scraper = _make_scraper()
        items = [_make_api_item(doc_id=1, tipo=2), _make_api_item(doc_id=2, tipo=9)]
        data = {"results": items}
        result = await scraper._get_docs_links("https://example.com/api/", data=data)
        assert result[0]["type"] == scraper._resolve_type_name(2)
        assert result[1]["type"] == scraper._resolve_type_name(9)


class TestProcessPdf:
    def test_build_pdf_fetch_urls_prefers_https_for_same_host(self):
        scraper = _make_scraper()
        urls = scraper._build_pdf_fetch_urls(
            "http://sapl3.al.pb.leg.br/media/sapl/public/normajuridica/2025/1/lei.pdf"
        )
        assert urls == [
            "https://sapl3.al.pb.leg.br/media/sapl/public/normajuridica/2025/1/lei.pdf",
            "http://sapl3.al.pb.leg.br/media/sapl/public/normajuridica/2025/1/lei.pdf",
        ]
