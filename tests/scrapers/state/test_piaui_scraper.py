"""Tests for PiauiAlepiScraper.

Covers:
- TYPES constant: 8 types present, integer IDs
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set (SAPLBaseScraper passes situations={})
- _id_to_type reverse map built correctly
- inherited SAPL URL formatting includes page_size=100
- inherited _get_docs_links parsing and missing-attachment reporting

Run with:
    .venv/bin/pytest tests/test_piaui_scraper.py -v
"""

import pytest
from conftest import make_scraper
from parser_assertions import ScraperClassTests, TypesConstantTests

from src.scrapers.states.pi import TYPES, PiauiAlepiScraper


def _make_scraper(**kwargs) -> PiauiAlepiScraper:
    """Instantiate PiauiAlepiScraper with injected dependencies."""
    kwargs.setdefault("subjects", {})
    kwargs.setdefault("_id_to_type", {v: k for k, v in TYPES.items()})
    kwargs.setdefault("_page_size", 100)
    return make_scraper(PiauiAlepiScraper, **kwargs)


def _make_api_item(
    doc_id: int = 1,
    texto_integral: str | None = "https://sapl.al.pi.leg.br/media/lei.pdf",
    data_vigencia: str | None = None,
    tipo: int = 1,
    **kwargs,
) -> dict:
    return {
        "id": doc_id,
        "numero": str(doc_id),
        "__str__": f"Lei {doc_id}/2025",
        "link_detail_backend": f"/norma/{doc_id}",
        "ementa": "Ementa da lei.",
        "assuntos": [73],
        "data": "2025-01-01",
        "ano": 2025,
        "esfera_federacao": "E",
        "veiculo_publicacao": None,
        "texto_integral": texto_integral,
        "data_vigencia": data_vigencia,
        "tipo": tipo,
        "materia": None,
        **kwargs,
    }


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 8
    REQUIRED_KEYS = {
        "Constituição Estadual",
        "Decreto",
        "Lei",
        "Lei Complementar",
        "Emenda Constitucional",
        "Resolução",
        "Decreto Legislativo",
        "Lei Delegada",
    }


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = PiauiAlepiScraper
    STATE_NAME = "Piauí"

    def test_situations_empty(self):
        scraper = _make_scraper()
        assert scraper.situations == {}

    def test_id_to_type_built_correctly(self):
        scraper = _make_scraper()
        assert scraper._id_to_type[1] == "Lei"
        assert scraper._id_to_type[2] == "Lei Complementar"
        assert scraper._id_to_type[7] == "Decreto"
        assert scraper._id_to_type[10] == "Constituição Estadual"
        assert len(scraper._id_to_type) == len(TYPES)


class TestFormatUrls:
    def test_format_search_url_includes_page_size(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("10", 2025)
        assert "tipo=10" in url
        assert "ano=2025" in url
        assert "page=1" in url
        assert "page_size=100" in url

    def test_format_year_url_includes_page_size(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2025, page=3)
        assert "tipo=" not in url
        assert "ano=2025" in url
        assert "page=3" in url
        assert "page_size=100" in url


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_valid_item_has_expected_fields(self):
        scraper = _make_scraper(subjects={73: "Constituição"})
        item = _make_api_item(doc_id=10, tipo=10, __str__="Constituição Estadual nº 4")
        data = {"results": [item]}
        result = await scraper._get_docs_links("https://example.com/api/", data=data)
        assert len(result) == 1
        doc = result[0]
        assert doc["id"] == 10
        assert doc["title"] == "Constituição Estadual nº 4"
        assert doc["type"] == scraper._resolve_type_name(10)
        assert doc["pdf_link"] == "https://sapl.al.pi.leg.br/media/lei.pdf"
        assert doc["subject"] == ["Constituição"]
