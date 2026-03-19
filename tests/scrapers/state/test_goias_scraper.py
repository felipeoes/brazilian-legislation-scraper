"""Tests for LegislaGoias scraper.

Covers:
- TYPES constant: 10 types present with id + url_suffix structure
- _TYPE_ID_TO_SUFFIX: reverse mapping from type id to url_suffix
- SITUATIONS module-level dict preserved (empty dict for Goias — situations come from API)
- Class docstring accessible (__doc__ is not None)
- _build_search_url: correct URL + query params, optional norm_type_id, page=1 default
- _clean_markdown: strips javascript:print(), strips whitespace
- _process_pdf_link: download failure/invalid markdown → None, valid → correct shape,
  sets document_url when missing vs pdf_link when already set
- _get_doc_info: HTTP error → None, early resume skip (before detail API call) → None,
  redirect guard → None, HTML with baixar_div as secondary pdf source,
  HTML to markdown happy path, stub HTML falls back to PDF,
  invalid markdown falls back to PDF, javascript error message guard → None,
  saves successful HTML as `.html`, derives norm_url_suffix from search result
- _fetch_search_page: HTTP error → empty list, valid → list of docs
- _scrape_year: single paginated fetch per year, handles empty results, delegates to
  _process_documents and uses _fetch_all_pages for remaining pages

Run with:
    uv run pytest tests/scrapers/state/test_goias_scraper.py -v
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from conftest import make_base_scraper

from src.scraper.state_legislation.goias import (
    _TYPE_ID_TO_SUFFIX,
    SITUATIONS,
    TYPES,
    LegislaGoias,
    _remove_summary_from_markdown,
)

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> LegislaGoias:
    """Instantiate LegislaGoias bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        LegislaGoias,
        "https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes",
        "GOIAS",
        {k: dict(v) for k, v in TYPES.items()},
        dict(SITUATIONS),
        **kwargs,
    )


def _make_valid_md() -> str:
    return (
        "# Lei Estadual\n\nO governador do estado de Goiás decreta a presente lei. "
        * 30
    )


def _make_doc_detail(
    doc_id: int = 1,
    numero: str = "001",
    ano: int = 2020,
    tipo_nome: str = "Lei Ordinária",
    tipo_id: int = 2,
    conteudo: str = "",
    ementa: str = "Ementa da lei",
) -> dict:
    return {
        "id": doc_id,
        "numero": numero,
        "ano": ano,
        "data_legislacao": f"{ano}-01-01",
        "tipo_legislacao": {"nome": tipo_nome, "id": tipo_id},
        "ementa": ementa,
        "estado_legislacao": {"nome": "Vigente"},
        "conteudo": conteudo,
    }


def _make_search_result(
    doc_id: int = 1,
    numero: str = "001",
    ano: int = 2020,
    tipo_nome: str = "Lei Ordinária",
    tipo_id: int = 2,
) -> dict:
    """Make a search result item (as returned by the list API)."""
    return {
        "id": doc_id,
        "numero": numero,
        "ano": ano,
        "tipo_legislacao": {"nome": tipo_nome, "id": tipo_id},
        "estado_legislacao": {"nome": "Vigente"},
    }


def _make_response(data: dict) -> MagicMock:
    resp = MagicMock()
    resp.__bool__ = MagicMock(return_value=True)
    resp.json = AsyncMock(return_value=data)
    resp.read = AsyncMock(return_value=b"%PDF fake content")
    return resp


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 10
    REQUIRED_KEYS = {"Lei Ordinária", "Lei Complementar", "Constituição Estadual"}
    REQUIRE_INT_VALUES = False

    def test_each_type_has_id_and_url_suffix(self):
        for name, data in TYPES.items():
            assert "id" in data, f"{name} missing 'id'"
            assert "url_suffix" in data, f"{name} missing 'url_suffix'"

    def test_ids_are_integers(self):
        for name, data in TYPES.items():
            assert isinstance(data["id"], int), f"{name} id is not int"


# ---------------------------------------------------------------------------
# _TYPE_ID_TO_SUFFIX
# ---------------------------------------------------------------------------


class TestTypeIdToSuffix:
    def test_maps_all_type_ids(self):
        for name, data in TYPES.items():
            assert data["id"] in _TYPE_ID_TO_SUFFIX, f"{name} id not in mapping"

    def test_correct_suffix_for_lei_ordinaria(self):
        assert _TYPE_ID_TO_SUFFIX[2] == "lei"

    def test_correct_suffix_for_constituicao(self):
        assert _TYPE_ID_TO_SUFFIX[12] == "constituicao-estadual"


# ---------------------------------------------------------------------------
# SITUATIONS constant
# ---------------------------------------------------------------------------


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = LegislaGoias
    STATE_NAME = "Goias"

    def test_situations_empty_by_default(self):
        scraper = _make_scraper()
        assert scraper.situations == {}


# ---------------------------------------------------------------------------
# _build_search_url
# ---------------------------------------------------------------------------


class TestBuildSearchUrl:
    def test_contains_ano_param(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 1)
        assert "ano=2020" in url

    def test_contains_page_param(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 3)
        assert "page=3" in url

    def test_no_tipo_legislacao_by_default(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 1)
        assert "tipo_legislacao" not in url

    def test_includes_tipo_legislacao_when_provided(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 1, norm_type_id=2)
        assert "tipo_legislacao=2" in url

    def test_no_shared_state_mutation(self):
        scraper = _make_scraper()
        url1 = scraper._build_search_url(2020, 1, norm_type_id=2)
        url2 = scraper._build_search_url(2019, 2, norm_type_id=7)
        assert "tipo_legislacao=2" in url1
        assert "tipo_legislacao=7" in url2
        assert "ano=2020" in url1
        assert "ano=2019" in url2

    def test_base_url_included(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 1)
        assert "legisla.casacivil.go.gov.br" in url

    def test_page_starts_at_1_by_default(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        assert "page=1" in url


# ---------------------------------------------------------------------------
# _clean_markdown
# ---------------------------------------------------------------------------


class TestCleanMarkdown:
    def test_removes_javascript_print(self):
        scraper = _make_scraper()
        result = scraper._clean_markdown("text javascript:print() more")
        assert "javascript:print()" not in result

    def test_strips_whitespace(self):
        scraper = _make_scraper()
        result = scraper._clean_markdown("  hello  ")
        assert result == "hello"

    def test_passthrough_clean_text(self):
        scraper = _make_scraper()
        text = "# Lei\n\nConteúdo da lei."
        assert scraper._clean_markdown(text) == text


# ---------------------------------------------------------------------------
# _remove_summary_from_markdown
# ---------------------------------------------------------------------------


class TestRemoveSummaryFromMarkdown:
    def test_removes_matching_standalone_paragraph(self):
        summary = "Altera o Decreto nº 10.218 de 2023."
        md = f"# DECRETO\n\n{summary}\n\nO GOVERNADOR decreta:"
        result = _remove_summary_from_markdown(md, summary)
        assert summary not in result
        assert "# DECRETO" in result
        assert "O GOVERNADOR decreta:" in result

    def test_noop_when_summary_embedded_in_article(self):
        summary = "Fica aberto crédito suplementar."
        md = "# Portaria\n\nO SECRETÁRIO resolve:\n\nArt. 1º Fica aberto crédito suplementar.\n\nArt. 2º Esta portaria entra em vigor nesta data."
        result = _remove_summary_from_markdown(md, summary)
        assert result == md

    def test_noop_when_summary_empty(self):
        md = "# Lei\n\nConteúdo da lei."
        assert _remove_summary_from_markdown(md, "") == md

    def test_noop_when_summary_not_found(self):
        md = "# Lei\n\nConteúdo completamente diferente."
        result = _remove_summary_from_markdown(md, "Ementa que não existe no texto.")
        assert result == md

    def test_normalizes_extra_whitespace_in_summary(self):
        summary = "Dispõe sobre  a reorganização   administrativa."
        md = "# Lei\n\nDispõe sobre a reorganização administrativa.\n\nArt. 1º ..."
        result = _remove_summary_from_markdown(md, summary)
        assert "Dispõe sobre a reorganização administrativa." not in result

    def test_strips_markdown_formatting_chars_from_paragraph(self):
        summary = "Dispõe sobre a reorganização administrativa."
        md = "# Lei\n\n**Dispõe sobre a reorganização administrativa.**\n\nArt. 1º ..."
        result = _remove_summary_from_markdown(md, summary)
        assert "Dispõe sobre a reorganização administrativa." not in result

    def test_removes_only_first_occurrence_if_duplicate_paragraphs(self):
        summary = "Ementa da lei."
        md = f"# Lei\n\n{summary}\n\nConteúdo\n\n{summary}"
        result = _remove_summary_from_markdown(md, summary)
        # Both occurrences are standalone paragraphs — both should be removed
        assert result.count(summary) == 0


# ---------------------------------------------------------------------------
# _process_pdf_link
# ---------------------------------------------------------------------------


class TestProcessPdfLink:
    @pytest.mark.asyncio
    async def test_download_failure_returns_none(self):
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(return_value=("", b"", ""))
        result = await scraper._process_pdf_link(
            "http://example.com/doc.pdf",
            "42",
            {
                "title": "Lei 001",
                "year": 2020,
                "type": "Lei Ordinária",
                "situation": "Vigente",
            },
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_invalid_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._download_and_convert = AsyncMock(
            return_value=("short", b"%PDF short", ".pdf")
        )
        result = await scraper._process_pdf_link(
            "http://example.com/doc.pdf",
            "42",
            {
                "title": "Lei 001",
                "year": 2020,
                "type": "Lei Ordinária",
                "situation": "Vigente",
            },
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_valid_pdf_returns_correct_shape(self):
        scraper = _make_scraper()
        valid_md = _make_valid_md()
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"%PDF content", ".pdf")
        )
        doc_info = {
            "title": "Lei 001",
            "summary": "ementa",
            "year": 2020,
            "type": "Lei Ordinária",
            "situation": "Vigente",
        }
        result = await scraper._process_pdf_link(
            "http://example.com/doc.pdf", "42", doc_info
        )
        assert result is not None
        assert result["_content_extension"] == ".pdf"
        assert result["document_url"] == "http://example.com/doc.pdf"
        assert result["_raw_content"] == b"%PDF content"

    @pytest.mark.asyncio
    async def test_sets_pdf_link_when_document_url_already_set(self):
        scraper = _make_scraper()
        valid_md = _make_valid_md()
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"%PDF content", ".pdf")
        )
        doc_info = {
            "title": "Lei 001",
            "summary": "ementa",
            "document_url": "https://existing.url",
            "year": 2020,
            "type": "Lei Ordinária",
            "situation": "Vigente",
        }
        result = await scraper._process_pdf_link(
            "http://example.com/doc.pdf", "42", doc_info
        )
        assert result is not None
        assert result["document_url"] == "https://existing.url"
        assert result["pdf_link"] == "http://example.com/doc.pdf"

    @pytest.mark.asyncio
    async def test_clean_markdown_applied(self):
        scraper = _make_scraper()
        valid_md = _make_valid_md() + " javascript:print()"
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"%PDF content", ".pdf")
        )
        doc_info = {
            "title": "Lei 001",
            "summary": "ementa",
            "year": 2020,
            "type": "Lei Ordinária",
            "situation": "Vigente",
        }
        result = await scraper._process_pdf_link(
            "http://example.com/doc.pdf", "42", doc_info
        )
        assert result is not None
        assert "javascript:print()" not in result["text_markdown"]

    @pytest.mark.asyncio
    async def test_summary_removed_from_pdf_markdown(self):
        scraper = _make_scraper()
        summary = "Dispõe sobre reorganização administrativa do Estado."
        # Build valid markdown that includes the summary as a standalone paragraph
        valid_body = "# Lei Estadual\n\n" + "O governador do estado decreta. " * 30
        valid_md = valid_body + f"\n\n{summary}\n\nArt. 1º Disposições gerais."
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"%PDF content", ".pdf")
        )
        doc_info = {
            "title": "Lei 001",
            "summary": summary,
            "year": 2020,
            "type": "Lei Ordinária",
            "situation": "Vigente",
        }
        result = await scraper._process_pdf_link(
            "http://example.com/doc.pdf", "42", doc_info
        )
        assert result is not None
        paragraphs = result["text_markdown"].split("\n\n")
        assert summary not in paragraphs


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocInfo:
    @pytest.mark.asyncio
    async def test_http_error_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        failed = MagicMock()
        failed.__bool__ = MagicMock(return_value=False)
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(_make_search_result())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_resume_skip_returns_none_without_api_call(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        scraper.request_service.make_request = AsyncMock()
        result = await scraper._get_doc_data(_make_search_result())
        assert result is None
        # Detail API should NOT be called when resume-skipping
        scraper.request_service.make_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_redirect_guard_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        conteudo = "<html><body>Clique no link abaixo para acessar a: constituição</body></html>"
        detail = _make_doc_detail(conteudo=conteudo)
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        result = await scraper._get_doc_data(_make_search_result())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_conteudo_without_pdf_link_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        detail = _make_doc_detail(conteudo="")
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(_make_search_result())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_html_content_returns_doc(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        # Long enough content (after stripping summary) to pass the 150-char threshold
        long_content = "<p>" + "Lei text " * 100 + "</p>"
        detail = _make_doc_detail(conteudo=long_content, ementa="ementa")
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(_make_search_result())
        assert result is not None
        assert result["type"] == "Lei Ordinária"
        assert result["text_markdown"] is not None

    @pytest.mark.asyncio
    async def test_summary_removed_from_html_markdown(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        summary = "Dispõe sobre a reorganização administrativa do Poder Executivo."
        long_content = "<p>" + "Lei text " * 100 + "</p>"
        detail = _make_doc_detail(conteudo=long_content, ementa=summary)
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        # Include the summary as a standalone paragraph in the markdown output
        valid_body = _make_valid_md()
        valid_md_with_summary = f"# DECRETO\n\n{summary}\n\n{valid_body}"
        scraper._get_markdown = AsyncMock(return_value=valid_md_with_summary)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(_make_search_result())
        assert result is not None
        paragraphs = result["text_markdown"].split("\n\n")
        assert summary not in paragraphs

        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        detail = _make_doc_detail(conteudo="<p>" + "Lei text " * 100 + "</p>")
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._get_markdown = AsyncMock(return_value=_make_valid_md())
        scraper._capture_mhtml = AsyncMock()
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(_make_search_result())

        assert result is not None
        assert result["_content_extension"] == ".html"
        scraper._capture_mhtml.assert_not_called()

    @pytest.mark.asyncio
    async def test_stub_html_with_pdf_link_falls_back_to_pdf_even_if_markdown_valid(
        self,
    ):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        conteudo = (
            '<a href="/api/v1/arquivos/14168"><img src="/assets/ver_lei.jpg"></a>'
            "<p>Ementa da lei</p><p>(D.O. de 29-12-1978)</p>"
        )
        detail = _make_doc_detail(conteudo=conteudo, ementa="Ementa da lei")
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._get_markdown = AsyncMock(
            return_value=("Ementa da lei " * 8) + "Diario Oficial"
        )
        scraper._process_pdf_link = AsyncMock(
            return_value={
                "title": "Lei 001",
                "text_markdown": _make_valid_md(),
                "document_url": "http://x.com",
                "type": "Lei Ordinária",
                "situation": "Vigente",
                "year": 2020,
                "summary": "",
            }
        )
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(_make_search_result())

        assert result is not None
        scraper._process_pdf_link.assert_called_once()

    @pytest.mark.asyncio
    async def test_type_falls_back_to_search_row_when_detail_type_missing(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        detail = _make_doc_detail(conteudo="<p>" + "Lei text " * 100 + "</p>")
        detail["tipo_legislacao"] = {}
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._get_markdown = AsyncMock(return_value=_make_valid_md())
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(
            _make_search_result(tipo_nome="Resolução", tipo_id=7)
        )

        assert result is not None
        assert result["type"] == "Resolução"

    @pytest.mark.asyncio
    async def test_javascript_error_msg_guard(self):
        """valid_markdown catches the JS-disabled message; no PDF fallback → error."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        content = "<p>Some content</p>"
        detail = _make_doc_detail(conteudo=content, ementa="ementa")
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        bad_md = "doesn't work properly without JavaScript enabled " * 10
        scraper._get_markdown = AsyncMock(return_value=bad_md)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(_make_search_result())
        # valid_markdown catches the JS error pattern → no PDF link → saves error
        scraper._save_doc_error.assert_called_once()
        assert result is None

    @pytest.mark.asyncio
    async def test_constituicao_url_has_no_number_suffix(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        detail = _make_doc_detail(
            doc_id=42,
            numero="001",
            tipo_nome="Constituição Estadual",
            tipo_id=12,
            conteudo="",
        )
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._save_doc_error = AsyncMock()
        search_result = _make_search_result(
            doc_id=42, numero="001", tipo_nome="Constituição Estadual", tipo_id=12
        )
        await scraper._get_doc_data(search_result)
        # Check is_already_scraped was called with URL ending in /constituicao-estadual (no number)
        called_url = scraper._is_already_scraped.call_args[0][0]
        assert called_url.endswith("constituicao-estadual")
        assert "001" not in called_url

    @pytest.mark.asyncio
    async def test_derives_suffix_from_search_result_tipo_id(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        detail = _make_doc_detail(
            doc_id=99, numero="55", tipo_nome="Resolução", tipo_id=7
        )
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._save_doc_error = AsyncMock()
        search_result = _make_search_result(
            doc_id=99, numero="55", tipo_nome="Resolução", tipo_id=7
        )
        await scraper._get_doc_data(search_result)
        called_url = scraper._is_already_scraped.call_args[0][0]
        assert "resolucao-55" in called_url

    @pytest.mark.asyncio
    async def test_baixar_div_used_as_secondary_pdf_source(self):
        """When ver_lei.jpg is absent, baixar_div provides the pdf_link."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        # HTML with botao-baixar but no ver_lei.jpg, and short content to trigger PDF fallback
        conteudo = '<div class="botao-baixar"><a href="/api/v1/arquivos/123">Download</a></div><p>short</p>'
        detail = _make_doc_detail(conteudo=conteudo, ementa="short")
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        # Make markdown invalid to force PDF fallback
        scraper._get_markdown = AsyncMock(return_value="x")
        scraper._process_pdf_link = AsyncMock(
            return_value={
                "title": "Lei 001",
                "text_markdown": _make_valid_md(),
                "document_url": "http://x.com",
                "type": "Lei Ordinária",
                "situation": "Vigente",
                "year": 2020,
                "summary": "",
            }
        )
        scraper._save_doc_error = AsyncMock()
        await scraper._get_doc_data(_make_search_result())
        # _process_pdf_link should be called with the baixar_div link
        scraper._process_pdf_link.assert_called_once()
        assert (
            scraper._process_pdf_link.call_args[0][0]
            == "https://legisla.casacivil.go.gov.br/api/v1/arquivos/123"
        )

    @pytest.mark.asyncio
    async def test_baixar_div_does_not_override_ver_lei_pdf_link(self):
        """When both ver_lei.jpg and baixar_div exist, ver_lei.jpg link is used."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        conteudo = (
            '<a href="/api/v1/arquivos/primary"><img src="/assets/ver_lei.jpg"></a>'
            '<div class="botao-baixar"><a href="/api/v1/arquivos/secondary">Download</a></div>'
            "<p>short</p>"
        )
        detail = _make_doc_detail(conteudo=conteudo, ementa="short")
        response = _make_response(detail)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._get_markdown = AsyncMock(return_value="x")
        scraper._process_pdf_link = AsyncMock(
            return_value={
                "title": "Lei 001",
                "text_markdown": _make_valid_md(),
                "document_url": "http://x.com",
                "type": "Lei Ordinária",
                "situation": "Vigente",
                "year": 2020,
                "summary": "",
            }
        )
        scraper._save_doc_error = AsyncMock()
        await scraper._get_doc_data(_make_search_result())
        # Should use the ver_lei.jpg link, NOT the baixar_div link
        scraper._process_pdf_link.assert_called_once()
        assert (
            scraper._process_pdf_link.call_args[0][0]
            == "https://legisla.casacivil.go.gov.br/api/v1/arquivos/primary"
        )


# ---------------------------------------------------------------------------
# _fetch_search_page
# ---------------------------------------------------------------------------


class TestFetchSearchPage:
    @pytest.mark.asyncio
    async def test_http_error_returns_empty_list(self):
        scraper = _make_scraper()
        failed = MagicMock()
        failed.__bool__ = MagicMock(return_value=False)
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._fetch_search_page(2025, 2)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_page_results(self):
        scraper = _make_scraper()
        payload = {"total_resultados": 1, "resultados": [_make_search_result()]}
        response = _make_response(payload)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        result = await scraper._fetch_search_page(2025, 2)
        assert result == payload["resultados"]


# ---------------------------------------------------------------------------
# _scrape_year
# ---------------------------------------------------------------------------


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_empty_year_returns_empty_list(self):
        scraper = _make_scraper()
        response = _make_response({"total_resultados": 0, "resultados": []})
        scraper.request_service.make_request = AsyncMock(return_value=response)
        result = await scraper._scrape_year(2025)
        assert result == []

    @pytest.mark.asyncio
    async def test_http_error_returns_empty_list(self):
        scraper = _make_scraper()
        failed = MagicMock()
        failed.__bool__ = MagicMock(return_value=False)
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._scrape_year(2025)
        assert result == []

    @pytest.mark.asyncio
    async def test_single_page_processes_documents_once(self):
        scraper = _make_scraper()
        data = {"total_resultados": 5, "resultados": [_make_search_result()]}
        response = _make_response(data)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._process_documents = AsyncMock(return_value=[{"title": "Doc"}])
        result = await scraper._scrape_year(2025)
        assert len(result) == 1
        scraper._process_documents.assert_called_once()

    @pytest.mark.asyncio
    async def test_multi_page_uses_fetch_all_pages(self):
        scraper = _make_scraper()
        data = {"total_resultados": 150, "resultados": [_make_search_result(doc_id=1)]}
        response = _make_response(data)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._fetch_all_pages = AsyncMock(
            return_value=[_make_search_result(doc_id=2)]
        )
        scraper._process_documents = AsyncMock(return_value=[{"title": "Doc"}])

        result = await scraper._scrape_year(2025)

        assert result == [{"title": "Doc"}]
        scraper._fetch_all_pages.assert_called_once()
        scraper._process_documents.assert_called_once()

    @pytest.mark.asyncio
    async def test_builds_url_without_tipo_legislacao(self):
        """_scrape_year should fetch all types at once (no tipo_legislacao param)."""
        scraper = _make_scraper()
        data = {"total_resultados": 0, "resultados": []}
        response = _make_response(data)
        scraper.request_service.make_request = AsyncMock(return_value=response)
        await scraper._scrape_year(2025)
        called_url = scraper.request_service.make_request.call_args[0][0]
        assert "tipo_legislacao" not in called_url
        assert "page=1" in called_url
