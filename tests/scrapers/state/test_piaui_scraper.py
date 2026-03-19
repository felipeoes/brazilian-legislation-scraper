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

from unittest.mock import AsyncMock

import pytest
from base_tests import ScraperClassTests, TypesConstantTests
from conftest import make_base_scraper

from src.scraper.state_legislation.piaui import TYPES, PiauiAlepiScraper


def _make_scraper(**kwargs) -> PiauiAlepiScraper:
    """Instantiate PiauiAlepiScraper bypassing __init__ (no network, no I/O)."""
    kwargs.setdefault("subjects", {})
    kwargs.setdefault("_id_to_type", {v: k for k, v in TYPES.items()})
    kwargs.setdefault("_page_size", 100)
    return make_base_scraper(
        PiauiAlepiScraper,
        "https://sapl.al.pi.leg.br",
        "PIAUI",
        TYPES,
        **kwargs,
    )


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
        assert doc["tipo_id"] == 10
        assert doc["pdf_link"] == "https://sapl.al.pi.leg.br/media/lei.pdf"
        assert doc["subject"] == ["Constituição"]

    @pytest.mark.asyncio
    async def test_missing_texto_integral_is_reported_and_skipped(self):
        scraper = _make_scraper()
        scraper._save_doc_error = AsyncMock()
        item = _make_api_item(
            doc_id=11,
            texto_integral=None,
            tipo=10,
            __str__="Constituição Estadual nº 4",
        )
        data = {"results": [item]}

        result = await scraper._get_docs_links("https://example.com/api/", data=data)

        assert result == []
        scraper._save_doc_error.assert_awaited_once_with(
            title="Constituição Estadual nº 4",
            year=2025,
            situation="Não consta revogação expressa",
            norm_type="Constituição Estadual",
            html_link="https://sapl.al.pi.leg.br/norma/11",
            error_message="SAPL API returned no texto_integral attachment",
            norma_id=11,
            materia_id=None,
            api_detail_url="https://sapl.al.pi.leg.br/api/norma/normajuridica/11",
        )

    @pytest.mark.asyncio
    async def test_pre_parsed_data_skips_request(self):
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock()
        item = _make_api_item(doc_id=5)
        data = {"results": [item]}

        result = await scraper._get_docs_links("https://example.com/api/", data=data)

        scraper.request_service.make_request.assert_not_called()
        assert len(result) == 1


class TestProcessPdf:
    @pytest.mark.asyncio
    async def test_process_pdf_strips_sei_footer_noise(self):
        scraper = _make_scraper()
        noisy_md = (
            "ALEPI - SEGOV-PI\n"
            "Av. Antonino Freire, 1450 Palácio de Karnak - Bairro Centro\n"
            "http://www.pi.gov.br\n"
            "PROPOSIÇÃO 2025/SEGOV-PI/GAB/PROTO-ALEPI\n\n"
            "LEI Nº 1, DE 01 DE JANEIRO DE 2025\n\n"
            "Art. 1º Texto principal da norma.\n\n"
            "Documento assinado eletronicamente por FULANO DE TAL.\n"
            "Piauí, em 01/01/2025, às 10:00, conforme horário oficial de Brasília.\n"
            "fundamento no Cap. III, Art. 14 do Decreto Estadual nº 18.142.\n"
            "LEI 1 (0001)         SEI 00010.000001/2025-10 / pg. 1\n"
            "\fA autenticidade deste documento pode ser conferida no site\n"
            "https://sei.pi.gov.br/sei/controlador_externo.php?\n"
            "acao=documento_conferir&id_orgao_acesso_externo=0\n"
            "código verificador 0001 e o código CRC ABCD1234.\n\n"
            "Art. 2º Texto que continua na página seguinte com conteúdo suficiente.\n\n"
            "GOVERNO DO ESTADO DO PIAUÍ\n"
            "SECRETARIA DE GOVERNO DO ESTADO DO PIAUÍ - SEGOV-PI\n"
            "http://www.pi.gov.br\n"
            "EXPEDIENTE 2025/SEGOV-PI/GAB/PROTO-ALEPI\n"
            "OFÍCIO PRES. SGM Nº 10/2025\n"
        )
        scraper._download_and_convert = AsyncMock(
            return_value=(noisy_md, b"%PDF", ".pdf")
        )

        result = await scraper._process_pdf("https://example.com/lei.pdf", 2025)

        assert result is not None
        assert "ALEPI - SEGOV-PI" not in result["text_markdown"]
        assert "PROPOSIÇÃO 2025/SEGOV-PI/GAB/PROTO-ALEPI" not in result["text_markdown"]
        assert "http://www.pi.gov.br" not in result["text_markdown"]
        assert "EXPEDIENTE 2025/SEGOV-PI/GAB/PROTO-ALEPI" not in result["text_markdown"]
        assert "OFÍCIO PRES. SGM Nº 10/2025" not in result["text_markdown"]
        assert "Documento assinado eletronicamente" not in result["text_markdown"]
        assert "A autenticidade deste documento" not in result["text_markdown"]
        assert "controlador_externo.php" not in result["text_markdown"]
        assert "SEI 00010.000001/2025-10 / pg. 1" not in result["text_markdown"]
        assert "Art. 1º Texto principal da norma." in result["text_markdown"]
        assert (
            "Art. 2º Texto que continua na página seguinte" in result["text_markdown"]
        )
