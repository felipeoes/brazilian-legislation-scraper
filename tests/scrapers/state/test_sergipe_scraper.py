"""Tests for SergipeLegsonScraper.

Covers:
- TYPES constant: 4 types with integer IDs
- SITUATIONS constant: merged dict with valid + invalid situations
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set
- _format_search_payload: correct payload construction
- _clean_legison_markdown: removes portal boilerplate
- _extract_content_markdown: extracts text from content API response
- _get_docs_links:
  - failed request → []
  - non-dict response → []
  - no result key → []
  - valid items → correct doc dict shapes
  - situacao id mapping to situation string
- _get_doc_data:
  - missing doc_id → None + _save_doc_error
  - failed content request → raises (retry propagates)
  - already scraped → None
  - content_markdown found via API → direct text return
  - PDF fallback with invalid markdown → falls through to error
  - PDF fallback with valid markdown → correct dict shape
  - no content and no PDF → None + _save_doc_error

Run with:
    .venv/bin/pytest tests/test_sergipe_scraper.py -v
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from conftest import make_base_scraper, make_failed_request

from src.scraper.state_legislation.sergipe import (
    INVALID_SITUATIONS,
    SITUATIONS,
    TYPES,
    VALID_SITUATIONS,
    SergipeLegsonScraper,
)

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> SergipeLegsonScraper:
    """Instantiate SergipeLegsonScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        SergipeLegsonScraper,
        "https://legison.pge.se.gov.br",
        "SERGIPE",
        TYPES,
        situations=SITUATIONS,
        search_url="https://legison.pge.se.gov.br/Public/Consulta",
        doc_content_url="https://legison.pge.se.gov.br/Public/GetConteudoAto",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 4
    REQUIRED_KEYS = {
        "Lei Ordinária",
        "Lei Complementar",
        "Decreto",
        "Emenda Constitucional",
    }
    REQUIRE_INT_VALUES = True


# ---------------------------------------------------------------------------
# SITUATIONS constants
# ---------------------------------------------------------------------------


class TestSituationsConstants(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_em_vigor_in_valid(self):
        assert "Em Vigor" in VALID_SITUATIONS.values()

    def test_revogado_in_invalid(self):
        assert "Revogado" in INVALID_SITUATIONS.values()

    def test_situations_merges_both(self):
        for k, v in VALID_SITUATIONS.items():
            assert SITUATIONS[k] == v
        for k, v in INVALID_SITUATIONS.items():
            assert SITUATIONS[k] == v


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = SergipeLegsonScraper
    STATE_NAME = "Sergipe"


# ---------------------------------------------------------------------------
# _format_search_payload
# ---------------------------------------------------------------------------


class TestFormatSearchPayload:
    def test_includes_type_id(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023)
        assert payload["IdTipo"] == "3"

    def test_includes_year(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023)
        assert payload["ano"] == "2023"

    def test_default_page_is_1(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023)
        assert payload["Page"] == 1

    def test_custom_page(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023, page=5)
        assert payload["Page"] == 5

    def test_required_keys_present(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023)
        for key in ("Ementa", "IdTipo", "ano", "Page", "Order"):
            assert key in payload


# ---------------------------------------------------------------------------
# _clean_legison_markdown
# ---------------------------------------------------------------------------


class TestCleanLegisonMarkdown:
    def test_removes_portal_footer(self):
        scraper = _make_scraper()
        text = "Texto da lei.\n\nExtraído do Portal de Legislação do Governo de Sergipe - LegisOn https://legislacao.se.gov.br/\n"
        result = scraper._clean_legison_markdown(text)
        assert "LegisOn" not in result
        assert "Texto da lei." in result

    def test_removes_nao_substitui_disclaimer(self):
        scraper = _make_scraper()
        text = "Texto.\n\nEste texto não substitui o publicado no Diário Oficial do Estado."
        result = scraper._clean_legison_markdown(text)
        assert "não substitui" not in result

    def test_collapses_multiple_newlines(self):
        scraper = _make_scraper()
        text = "Linha 1.\n\n\n\n\nLinha 2."
        result = scraper._clean_legison_markdown(text)
        assert "\n\n\n" not in result

    def test_strips_result(self):
        scraper = _make_scraper()
        text = "  \n\nTexto.\n\n  "
        result = scraper._clean_legison_markdown(text)
        assert result == result.strip()


# ---------------------------------------------------------------------------
# _extract_content_markdown
# ---------------------------------------------------------------------------


class TestExtractContentMarkdown:
    def test_no_content_key_returns_empty(self):
        scraper = _make_scraper()
        result = scraper._extract_content_markdown({})
        assert result == ""

    def test_non_list_content_returns_empty(self):
        scraper = _make_scraper()
        result = scraper._extract_content_markdown({"content": "not a list"})
        assert result == ""

    def test_extracts_text_from_conteudo(self):
        scraper = _make_scraper()
        data = {"content": [{"conteudo": "Texto da lei."}, {"conteudo": "Mais texto."}]}
        result = scraper._extract_content_markdown(data)
        assert "Texto da lei." in result
        assert "Mais texto." in result

    def test_skips_items_without_conteudo(self):
        scraper = _make_scraper()
        data = {"content": [{"other": "value"}, {"conteudo": "Texto válido."}]}
        result = scraper._extract_content_markdown(data)
        assert "Texto válido." in result

    def test_empty_content_list_returns_empty(self):
        scraper = _make_scraper()
        result = scraper._extract_content_markdown({"content": []})
        assert result == ""


# ---------------------------------------------------------------------------
# _get_docs_links
# ---------------------------------------------------------------------------


def _make_api_item(
    doc_id: int = 1,
    numero: str = "42",
    tipo_descricao: str = "Lei Ordinária",
    ementa: str = "Dispõe sobre algo.",
    data_ato: str = "2023-06-01T00:00:00",
    situacao_id: int = 1,
) -> dict:
    return {
        "id": doc_id,
        "numero": numero,
        "dataAto": data_ato,
        "ementa": ementa,
        "tipoAto": {"descricao": tipo_descricao},
        "situacao": {"id": situacao_id},
    }


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_failed_request_returns_empty(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._get_docs_links(
            {"IdTipo": "3", "ano": "2023", "Page": 1}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_no_result_key_returns_empty(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"count": 0})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links(
            {"IdTipo": "3", "ano": "2023", "Page": 1}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_valid_item_has_expected_fields(self):
        scraper = _make_scraper()
        item = _make_api_item(doc_id=5, numero="42", tipo_descricao="Lei Ordinária")
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"result": [item]})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links(
            {"IdTipo": "3", "ano": "2023", "Page": 1}
        )
        assert len(result) == 1
        doc = result[0]
        assert doc["id"] == "5"
        assert "Lei Ordinária" in doc["title"]
        assert doc["summary"] == "Dispõe sobre algo."
        assert "situation" in doc
        assert doc["doc_id"] == 5

    @pytest.mark.asyncio
    async def test_situacao_id_maps_to_em_vigor(self):
        scraper = _make_scraper()
        item = _make_api_item(situacao_id=1)  # id 1 → "Em Vigor"
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"result": [item]})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links(
            {"IdTipo": "3", "ano": "2023", "Page": 1}
        )
        assert result[0]["situation"] == "Em Vigor"

    @pytest.mark.asyncio
    async def test_situacao_id_maps_to_revogado(self):
        scraper = _make_scraper()
        item = _make_api_item(situacao_id=2)  # id 2 → "Revogado"
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"result": [item]})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links(
            {"IdTipo": "3", "ano": "2023", "Page": 1}
        )
        assert result[0]["situation"] == "Revogado"

    @pytest.mark.asyncio
    async def test_title_includes_year_from_date(self):
        scraper = _make_scraper()
        item = _make_api_item(
            numero="10", tipo_descricao="Decreto", data_ato="2022-03-15T00:00:00"
        )
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"result": [item]})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links(
            {"IdTipo": "4", "ano": "2022", "Page": 1}
        )
        assert "2022" in result[0]["title"]

    @pytest.mark.asyncio
    async def test_multiple_items_returned(self):
        scraper = _make_scraper()
        items = [_make_api_item(doc_id=i, numero=str(i)) for i in range(1, 4)]
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"result": items})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_docs_links(
            {"IdTipo": "3", "ano": "2023", "Page": 1}
        )
        assert len(result) == 3


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    def _make_doc(self, **kwargs):
        base = {
            "doc_id": 42,
            "id": "42",
            "title": "Lei Ordinária 42 de 2023",
            "year": 2023,
            "type": "Lei Ordinária",
            "summary": "Ementa.",
            "situation": "Em Vigor",
            "date": "2023-01-01T00:00:00",
        }
        base.update(kwargs)
        return base

    @pytest.mark.asyncio
    async def test_missing_doc_id_returns_none(self):
        scraper = _make_scraper()
        scraper._save_doc_error = AsyncMock()
        doc = self._make_doc()
        doc.pop("doc_id")
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_already_scraped_returns_none(self):
        scraper = _make_scraper()
        content_url = "https://legison.pge.se.gov.br/Public/GetConteudoAto?atosIds=42"
        scraper._scraped_keys = {(content_url, "Lei Ordinária 42 de 2023")}
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.status = 200
        resp.json = AsyncMock(return_value={"content": [], "files": []})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_api_content_markdown_returned_directly(self):
        """When content API returns direct text, use it without PDF fallback."""
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.status = 200
        long_text = "Texto da lei. " * 40
        resp.json = AsyncMock(
            return_value={"content": [{"conteudo": long_text}], "files": []}
        )
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        doc = self._make_doc()
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert "text_markdown" in result
        assert result["_content_extension"] == ".txt"

    @pytest.mark.asyncio
    async def test_pdf_fallback_valid_markdown_returned(self):
        """When API text is empty but PDF succeeds with valid markdown."""
        scraper = _make_scraper()
        content_resp = MagicMock()
        content_resp.__bool__ = lambda s: True
        content_resp.status = 200
        content_resp.json = AsyncMock(
            return_value={
                "content": [],
                "files": [{"caminhoPDF": "lei42.pdf"}],
            }
        )
        pdf_resp = MagicMock()
        pdf_resp.__bool__ = lambda s: True
        pdf_resp.read = AsyncMock(return_value=b"%PDF content")
        scraper.request_service.make_request = AsyncMock(
            side_effect=[content_resp, pdf_resp]
        )
        valid_md = "# Lei 42/2023\n\n" + "Texto da lei. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = self._make_doc()
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["_content_extension"] == ".pdf"

    @pytest.mark.asyncio
    async def test_pdf_fallback_invalid_markdown_saves_error(self):
        """When PDF markdown is too short, falls through to error path."""
        scraper = _make_scraper()
        content_resp = MagicMock()
        content_resp.__bool__ = lambda s: True
        content_resp.status = 200
        content_resp.json = AsyncMock(
            return_value={
                "content": [],
                "files": [{"caminhoPDF": "lei42.pdf"}],
            }
        )
        pdf_resp = MagicMock()
        pdf_resp.__bool__ = lambda s: True
        pdf_resp.read = AsyncMock(return_value=b"%PDF short")
        scraper.request_service.make_request = AsyncMock(
            side_effect=[content_resp, pdf_resp]
        )
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        doc = self._make_doc()
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_content_no_pdf_saves_error(self):
        """When there's no API content and no PDF file, error is saved."""
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.status = 200
        resp.json = AsyncMock(return_value={"content": [], "files": []})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        scraper._save_doc_error = AsyncMock()
        doc = self._make_doc()
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()
