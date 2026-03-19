"""Tests for RoraimaAlerScraper.

Roraima uses SAPLBaseScraper with no overrides — all logic is inherited.
Covers:
- TYPES constant: 10 types with integer IDs
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set (SAPLBaseScraper passes situations={})
- situations is empty dict
- _format_search_url (inherited): correct SAPL API URL construction
- _get_docs_links (inherited):
  - failed request → []
  - no results key → []
  - items without texto_integral → skipped
  - data_vigencia present → situation = DEFAULT_INVALID_SITUATION
  - data_vigencia absent → situation = DEFAULT_VALID_SITUATION
  - valid item → correct doc dict shape
- _process_pdf (inherited base class):
  - _download_and_convert returns falsy markdown → None
  - _download_and_convert returns valid markdown → correct dict shape

Run with:
    .venv/bin/pytest tests/test_roraima_scraper.py -v
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, TypesConstantTests
from conftest import make_base_scraper, make_failed_request

from src.scraper.base.scraper import DEFAULT_INVALID_SITUATION, DEFAULT_VALID_SITUATION
from src.scraper.state_legislation.roraima import TYPES, RoraimaAlerScraper

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> RoraimaAlerScraper:
    """Instantiate RoraimaAlerScraper bypassing __init__ (no network, no I/O)."""
    kwargs.setdefault("subjects", {})
    kwargs.setdefault("_id_to_type", {v: k for k, v in TYPES.items()})
    kwargs.setdefault("_page_size", 100)
    return make_base_scraper(
        RoraimaAlerScraper,
        "https://sapl.al.rr.leg.br",
        "RORAIMA",
        TYPES,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 10
    REQUIRED_KEYS = {
        "Lei Ordinária",
        "Lei Complementar",
        "Constituição Estadual",
        "Emenda à Constituição",
        "Decreto Legislativo",
    }


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = RoraimaAlerScraper
    STATE_NAME = "Roraima"

    def test_situations_empty(self):
        scraper = _make_scraper()
        assert scraper.situations == {}

    def test_subjects_initialized_empty(self):
        scraper = _make_scraper()
        assert scraper.subjects == {}

    def test_id_to_type_built_correctly(self):
        scraper = _make_scraper()
        assert scraper._id_to_type[2] == "Lei Ordinária"
        assert scraper._id_to_type[3] == "Lei Complementar"
        assert len(scraper._id_to_type) == len(TYPES)


# ---------------------------------------------------------------------------
# _format_search_url (inherited from SAPLBaseScraper)
# ---------------------------------------------------------------------------


class TestFormatSearchUrl:
    def test_includes_tipo_param(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2, 2023)
        assert "tipo=2" in url

    def test_includes_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2, 2023)
        assert "ano=2023" in url

    def test_default_page_is_1(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2, 2023)
        assert "page=1" in url

    def test_includes_page_size(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2, 2023)
        assert "page_size=100" in url

    def test_custom_page(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(3, 2020, page=5)
        assert "page=5" in url

    def test_base_url_present(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(1, 2022)
        assert url.startswith("https://sapl.al.rr.leg.br")

    def test_api_path_present(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(1, 2022)
        assert "/api/norma/normajuridica/" in url


# ---------------------------------------------------------------------------
# _format_year_url (inherited from SAPLBaseScraper)
# ---------------------------------------------------------------------------


class TestFormatYearUrl:
    def test_no_tipo_param(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023)
        assert "tipo=" not in url

    def test_includes_year(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023)
        assert "ano=2023" in url

    def test_default_page_is_1(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023)
        assert "page=1" in url

    def test_includes_page_size(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023)
        assert "page_size=100" in url

    def test_custom_page(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023, page=5)
        assert "page=5" in url


# ---------------------------------------------------------------------------
# _get_docs_links (inherited from SAPLBaseScraper)
# ---------------------------------------------------------------------------


def _make_api_item(
    doc_id: int = 1,
    texto_integral: str | None = "https://sapl.al.rr.leg.br/media/lei.pdf",
    data_vigencia: str | None = None,
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
        "data_vigencia": data_vigencia,
        "tipo": tipo,
        **kwargs,
    }


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_failed_request_returns_empty(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._get_docs_links(
            "https://sapl.al.rr.leg.br/api/norma/normajuridica/?tipo=2&page=1&ano=2023"
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_no_results_returns_empty(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"results": []})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links("https://example.com/api/")
        assert result == []

    @pytest.mark.asyncio
    async def test_item_without_texto_integral_skipped(self):
        scraper = _make_scraper()
        scraper._save_doc_error = AsyncMock()
        item = _make_api_item(texto_integral=None)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"results": [item]})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links("https://example.com/api/")
        assert result == []
        scraper._save_doc_error.assert_awaited_once_with(
            title="Lei Ordinária 1/2023",
            year=2023,
            situation=DEFAULT_VALID_SITUATION,
            norm_type="Lei Ordinária",
            html_link="https://sapl.al.rr.leg.br/norma/1",
            error_message="SAPL API returned no texto_integral attachment",
            norma_id=1,
            materia_id=None,
            api_detail_url="https://sapl.al.rr.leg.br/api/norma/normajuridica/1",
        )

    @pytest.mark.asyncio
    async def test_data_vigencia_present_gives_invalid_situation(self):
        scraper = _make_scraper()
        item = _make_api_item(data_vigencia="2020-01-01")
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"results": [item]})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links("https://example.com/api/")
        assert len(result) == 1
        assert result[0]["situation"] == DEFAULT_INVALID_SITUATION

    @pytest.mark.asyncio
    async def test_no_data_vigencia_gives_valid_situation(self):
        scraper = _make_scraper()
        item = _make_api_item(data_vigencia=None)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"results": [item]})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links("https://example.com/api/")
        assert len(result) == 1
        assert result[0]["situation"] == DEFAULT_VALID_SITUATION

    @pytest.mark.asyncio
    async def test_valid_item_has_expected_fields(self):
        scraper = _make_scraper()
        item = _make_api_item(doc_id=7)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"results": [item]})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links("https://example.com/api/")
        doc = result[0]
        assert doc["id"] == 7
        assert doc["title"] == "Lei Ordinária 7/2023"
        assert doc["summary"] == "Ementa da lei."
        assert doc["pdf_link"] == "https://sapl.al.rr.leg.br/media/lei.pdf"
        assert doc["tipo_id"] == 2
        assert "date" in doc

    @pytest.mark.asyncio
    async def test_multiple_items_returned(self):
        scraper = _make_scraper()
        items = [_make_api_item(doc_id=i) for i in range(1, 4)]
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"results": items})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links("https://example.com/api/")
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_pre_parsed_data_skips_request(self):
        """When data is passed directly, no HTTP request should be made."""
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock()
        item = _make_api_item(doc_id=5)
        data = {"results": [item]}
        result = await scraper._get_docs_links("https://example.com/api/", data=data)
        scraper.request_service.make_request.assert_not_called()
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _process_pdf (inherited base class — uses _download_and_convert)
# ---------------------------------------------------------------------------


class TestProcessPdf:
    @pytest.mark.asyncio
    async def test_process_pdf_strips_roraima_sei_footer_noise(self):
        scraper = _make_scraper()
        noisy_md = (
            "24/10/2025,16:36\n"
            "SEI/GRR - 19847021 - Lei Complementar\n"
            "LEI COMPLEMENTAR N' 360, DE 24 DE OUTUBRO DE 2025\n\n"
            "Art. 1º Texto principal da norma.\n\n"
            "https://sei.rr.gov.br/sei/controlador.php?acao=documento.imprimir.web\n"
            "19847021v2\n"
            "1/3\n"
            "24/10/2025,16:37\n"
            "SEI/GRR - 19847021 - Lei Complementar\n\n"
            "§ 5º Texto que continua depois do rodapé.\n\n"
            "Praça do Centro Cívico, 202 Centro - Boa Vista - Roraima - Brasil\n"
            "ALE-RR na internet: https://al.rr.leg.br\n\n"
            "II - Inciso que continua depois do rodapé institucional.\n\n"
            "Palácio Senador Hélio Campos/RR, 24 de outubro de 2025\n"
            "(assinatura eletrônica)\n"
            "ANTONIO DENARIUM\n"
            "Governador do Estado de Roraima\n\n"
            "Roraima, em 24/10/2025, às 09:01, conforme Art. 5º, XIII,\n"
            '"b", do Decreto nº 27.971-E/2019\n'
            "A autenticidade do documento pode ser conferida no endereço\n"
            "informando o código verificador 19804223 e o código CRC D2F6BE06.\n"
            "13101.0002923/2025.11\n"
        )
        scraper._download_and_convert = AsyncMock(
            return_value=(noisy_md, b"%PDF", ".pdf")
        )

        result = await scraper._process_pdf(
            "https://example.com/lei.pdf",
            2025,
            title="Lei Complementar nº 360, de 24 de outubro de 2025",
        )

        assert result is not None
        assert result["text_markdown"].startswith(
            "LEI COMPLEMENTAR N' 360, DE 24 DE OUTUBRO DE 2025"
        )
        assert "https://sei.rr.gov.br" not in result["text_markdown"]
        assert "SEI/GRR" not in result["text_markdown"]
        assert "A autenticidade do documento" not in result["text_markdown"]
        assert "19847021v2" not in result["text_markdown"]
        assert "1/3" not in result["text_markdown"]
        assert "https://al.rr.leg.br" not in result["text_markdown"]
        assert "Praça do Centro Cívico" not in result["text_markdown"]
        assert "§ 5º Texto que continua depois do rodapé." in result["text_markdown"]
        assert (
            "II - Inciso que continua depois do rodapé institucional."
            in result["text_markdown"]
        )

    @pytest.mark.asyncio
    async def test_falsy_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(return_value=("", b"", ".pdf"))
        result = await scraper._process_pdf("https://example.com/lei.pdf", 2023)
        assert result is None

    @pytest.mark.asyncio
    async def test_whitespace_only_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(
            return_value=("   \n  ", b"bytes", ".pdf")
        )
        result = await scraper._process_pdf("https://example.com/lei.pdf", 2023)
        assert result is None

    @pytest.mark.asyncio
    async def test_short_markdown_returns_none(self):
        """_valid_markdown rejects text shorter than min_length (50 chars)."""
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(
            return_value=("short", b"%PDF", ".pdf")
        )
        result = await scraper._process_pdf("https://example.com/lei.pdf", 2023)
        assert result is None

    @pytest.mark.asyncio
    async def test_valid_markdown_returns_correct_shape(self):
        scraper = _make_scraper()
        valid_md = "# Lei Ordinária 1/2023\n\n" + "Texto da lei. " * 20
        raw_bytes = b"%PDF content"
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, raw_bytes, ".pdf")
        )
        result = await scraper._process_pdf("https://example.com/lei.pdf", 2023)
        assert result is not None
        assert result["text_markdown"] == valid_md.strip()
        assert result["document_url"] == "https://example.com/lei.pdf"
        assert result["raw_content"] == raw_bytes
        assert result["content_extension"] == ".pdf"

    @pytest.mark.asyncio
    async def test_download_convert_called_with_pdf_link(self):
        scraper = _make_scraper()
        valid_md = "# Lei\n\n" + "Texto. " * 20
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"bytes", ".pdf")
        )
        await scraper._process_pdf("https://example.com/lei.pdf", 2023)
        scraper._download_and_convert.assert_called_once_with(
            "https://example.com/lei.pdf"
        )
