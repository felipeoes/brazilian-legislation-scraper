"""Tests for AlagoasSefazScraper.

Covers:
- TYPES constant completeness and typo fix ("Constituição Estadual")
- _build_params: unfiltered (no especieLegislativa), correct date range, CAT017
- _build_url: page 1 returns bare base_url, page 2+ appends ?pagina=N
- _fetch_page_norms: happy-path returns rows, failure/malformed returns []
- _get_doc_data: resume skip, failed request, JSON exception, empty markdown,
  server-error caught by _valid_markdown, too-short markdown, correct result
  dict shape (id, number, title, type, summary, category, publication_date,
  text_markdown, document_url, _raw_content, _content_extension),
  type from tipoDocumento.descricao, _raw_content is decoded bytes,
  extension from nomeArquivo
- _scrape_year: [] on registrosTotais None, [] on failed first request,
  [] on malformed JSON, no pagination when total <= per_page,
  pagination triggered when total > per_page,
  _process_documents called with all rows, exactly 1 request when no pagination

Integration (live API):
- test_scrape_year_1942_count_matches_api
- test_scrape_year_1942_type_field_from_tipoDocumento
- test_scrape_year_1942_no_especieLegislativa_in_requests
- test_scrape_year_1942_mixed_types_in_single_request

Run with:
    .venv/bin/pytest tests/test_alagoas_scraper.py -v
"""

import base64
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import TypesConstantTests
from conftest import make_base_scraper, make_failed_request

from src.scraper.state_legislation.alagoas import TYPES, AlagoasSefazScraper

# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> AlagoasSefazScraper:
    """Instantiate AlagoasSefazScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        AlagoasSefazScraper,
        "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/administrativo/documento/consultar",
        "ALAGOAS",
        TYPES,
        situations={},
        view_doc_url="https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/documentos/visualizarDocumento?",
        **kwargs,
    )


def _make_api_response(
    rows: list[dict], total: int | None = None, per_page: int = 10
) -> MagicMock:
    """Mock aiohttp response returning a SEFAZ-shaped JSON payload."""
    resp = MagicMock()
    resp.__bool__ = lambda s: True
    payload = {
        "registrosTotais": total if total is not None else len(rows),
        "registrosPorPagina": per_page,
        "documentos": rows,
    }
    resp.json = AsyncMock(return_value=payload)
    return resp


def _make_doc_response(
    pdf_bytes: bytes, filename: str = "decreto_001.pdf"
) -> MagicMock:
    """Mock aiohttp response for a document download (base64 JSON payload)."""
    resp = MagicMock()
    resp.__bool__ = lambda s: True
    payload = {
        "arquivo": {
            "base64": base64.b64encode(pdf_bytes).decode(),
            "nomeArquivo": filename,
        }
    }
    resp.json = AsyncMock(return_value=payload)
    return resp


def _sample_row(**overrides) -> dict:
    base = {
        "setor": None,
        "categoria": {"descricao": "Gabinete Civil"},
        "tipoDocumento": {"descricao": "Decreto"},
        "numeroDocumento": 107,
        "dataPublicacao": "1942-12-31",
        "localPublicacaoDocumento": None,
        "dataDocumento": None,
        "descricaoCabecalho": None,
        "textoEmenta": "DISPÕE SOBRE ALGO.",
        "dataAlteracao": None,
        "listaParticaoDocumento": None,
        "listaImagemDocumento": None,
        "link": {"acess": "1", "key": "dDpCfi0+I1c=", "highlight": None},
        "generoDocumento": None,
        "imagemInicial": None,
        "_year": 1942,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 8
    REQUIRED_KEYS = {"Constituição Estadual", "Decreto"}
    REQUIRE_INT_VALUES = False

    def test_typo_is_fixed(self):
        """'Constituição' must be spelled correctly, not 'Consituição'."""
        assert "Consituição Estadual" not in TYPES
        assert "Constituição Estadual" in TYPES

    def test_decreto_maps_to_tip002(self):
        assert TYPES["Decreto"] == "TIP002"

    def test_lei_ordinaria_maps_to_tip043(self):
        assert TYPES["Lei Ordinária"] == "TIP043"


# ---------------------------------------------------------------------------
# _build_params
# ---------------------------------------------------------------------------


class TestBuildParams:
    def test_no_especieLegislativa_key(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert "especieLegislativa" not in params

    def test_periodo_inicial_contains_year(self):
        scraper = _make_scraper()
        params = scraper._build_params(1942)
        assert "1942-01-01" in params["periodoInicial"]

    def test_periodo_final_contains_year(self):
        scraper = _make_scraper()
        params = scraper._build_params(1942)
        assert "1942-12-31" in params["periodoFinal"]

    def test_codigo_categoria_is_cat017(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["codigoCategoria"] == "CAT017"

    def test_numero_is_none(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["numero"] is None

    def test_codigo_setor_is_none(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["codigoSetor"] is None


# ---------------------------------------------------------------------------
# _build_url
# ---------------------------------------------------------------------------


class TestBuildUrl:
    def test_page_1_returns_bare_base_url(self):
        scraper = _make_scraper()
        assert scraper._build_url(1) == scraper.base_url

    def test_default_page_is_1(self):
        scraper = _make_scraper()
        assert scraper._build_url() == scraper.base_url

    def test_page_2_appends_pagina_2(self):
        scraper = _make_scraper()
        url = scraper._build_url(2)
        assert url == scraper.base_url + "?pagina=2"

    def test_page_20_appends_pagina_20(self):
        scraper = _make_scraper()
        url = scraper._build_url(20)
        assert url.endswith("?pagina=20")

    def test_no_pagina_param_for_page_1(self):
        scraper = _make_scraper()
        assert "pagina" not in scraper._build_url(1)


# ---------------------------------------------------------------------------
# _fetch_page_norms
# ---------------------------------------------------------------------------


class TestFetchPageNorms:
    @pytest.mark.asyncio
    async def test_returns_rows_on_success(self):
        scraper = _make_scraper()
        rows = [_sample_row(numeroDocumento=i) for i in range(5)]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows)
        )
        params = scraper._build_params(1942)
        result = await scraper._fetch_page_norms(2, params)
        assert result == rows

    @pytest.mark.asyncio
    async def test_returns_empty_on_failed_request(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._fetch_page_norms(2, {})
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_malformed_json(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"unexpected": "shape"})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._fetch_page_norms(2, {})
        assert result == []

    @pytest.mark.asyncio
    async def test_url_contains_correct_page_number(self):
        scraper = _make_scraper()
        rows = [_sample_row()]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows)
        )
        await scraper._fetch_page_norms(5, {})
        call_url = scraper.request_service.make_request.call_args[0][0]
        assert "pagina=5" in call_url

    @pytest.mark.asyncio
    async def test_returns_empty_on_exception(self):
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        result = await scraper._fetch_page_norms(2, {})
        assert result == []


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_skips_already_scraped_document(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        result = await scraper._get_doc_data(_sample_row())
        assert result is None
        scraper.request_service.make_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_none_on_failed_request(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._get_doc_data(_sample_row())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_on_json_exception(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(side_effect=ValueError("bad json"))
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._get_doc_data(_sample_row())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_on_empty_markdown(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value="")
        result = await scraper._get_doc_data(_sample_row())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_on_server_error_in_markdown(self):
        """Server-error strings must be caught by _valid_markdown."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(b"%PDF-1.4 fake")
        )
        server_error = "failed to open stream: HTTP request failed! " * 5
        scraper._get_markdown = AsyncMock(return_value=server_error)
        result = await scraper._get_doc_data(_sample_row())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_on_too_short_markdown(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value="curto")
        result = await scraper._get_doc_data(_sample_row())
        assert result is None

    @pytest.mark.asyncio
    async def test_result_contains_type_from_tipoDocumento(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Esta lei estabelece critérios. " * 10
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)

        row = _sample_row(tipoDocumento={"descricao": "Lei Ordinária"})
        result = await scraper._get_doc_data(row)

        assert result is not None
        assert result["type"] == "Lei Ordinária"

    @pytest.mark.asyncio
    async def test_result_type_falls_back_to_filename_when_api_type_missing(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(
                b"%PDF-1.4 fake", filename="decreto_001.pdf"
            )
        )
        scraper._get_markdown = AsyncMock(
            return_value="Art. 1° Esta lei estabelece critérios. " * 10
        )

        row = _sample_row(tipoDocumento={"descricao": ""})
        result = await scraper._get_doc_data(row)

        assert result is not None
        assert result["type"] == "Decreto"

    @pytest.mark.asyncio
    async def test_result_dict_has_required_keys(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo legal completo e detalhado. " * 10
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)

        result = await scraper._get_doc_data(_sample_row())

        assert result is not None
        required = {
            "id",
            "number",
            "title",
            "type",
            "summary",
            "category",
            "publication_date",
            "text_markdown",
            "document_url",
            "_raw_content",
            "_content_extension",
        }
        assert required.issubset(result.keys())

    @pytest.mark.asyncio
    async def test_id_and_number_equal_numeroDocumento(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo legal completo. " * 10
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(b"%PDF-1.4 fake")
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)

        result = await scraper._get_doc_data(_sample_row(numeroDocumento=9999))

        assert result is not None
        assert result["id"] == 9999
        assert result["number"] == 9999

    @pytest.mark.asyncio
    async def test_raw_content_is_decoded_bytes(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo legal extenso. " * 10
        original_bytes = b"%PDF-1.4 real content here"
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(original_bytes)
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)

        result = await scraper._get_doc_data(_sample_row())

        assert result is not None
        assert result["_raw_content"] == original_bytes

    @pytest.mark.asyncio
    async def test_extension_from_nome_arquivo(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo legal extenso. " * 10
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(
                b"fake", filename="lei_complementar_042.pdf"
            )
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)

        result = await scraper._get_doc_data(_sample_row())

        assert result is not None
        assert result["_content_extension"] == ".pdf"

    @pytest.mark.asyncio
    async def test_document_url_contains_acess_and_key(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        long_md = "Art. 1° Conteúdo legal. " * 15
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_doc_response(b"fake")
        )
        scraper._get_markdown = AsyncMock(return_value=long_md)

        row = _sample_row(link={"acess": "42", "key": "abc=", "highlight": None})
        result = await scraper._get_doc_data(row)

        assert result is not None
        assert "acess=42" in result["document_url"]
        assert "key=" in result["document_url"]

    @pytest.mark.asyncio
    async def test_error_log_includes_year_from_injected_field(self):
        """_save_doc_error must receive the year injected as _year on doc_info."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)

        row = _sample_row(_year=1942)
        await scraper._get_doc_data(row)

        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert call_kwargs["year"] == 1942


# ---------------------------------------------------------------------------
# _scrape_year (unit — mocked network)
# ---------------------------------------------------------------------------


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_returns_empty_when_registrosTotais_is_none(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(
            return_value={
                "registrosTotais": None,
                "registrosPorPagina": None,
                "documentos": [],
            }
        )
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._scrape_year(1940)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_failed_first_request(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._scrape_year(1942)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_malformed_json(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.json = AsyncMock(return_value={"bad": "response"})
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        result = await scraper._scrape_year(1942)
        assert result == []

    @pytest.mark.asyncio
    async def test_no_pagination_when_total_fits_one_page(self):
        """When total <= per_page, _fetch_page_norms must not be called."""
        scraper = _make_scraper()
        rows = [_sample_row(numeroDocumento=i) for i in range(5)]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows, total=5, per_page=10)
        )
        scraper._process_documents = AsyncMock(return_value=[])
        scraper._fetch_page_norms = AsyncMock(return_value=[])

        await scraper._scrape_year(1942)

        scraper._fetch_page_norms.assert_not_called()

    @pytest.mark.asyncio
    async def test_pagination_triggered_when_total_exceeds_per_page(self):
        """When total > per_page, extra pages must be fetched."""
        scraper = _make_scraper()
        rows = [_sample_row(numeroDocumento=i) for i in range(10)]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows, total=25, per_page=10)
        )
        scraper._fetch_page_norms = AsyncMock(
            return_value=[_sample_row(numeroDocumento=99)]
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(1942)

        # total=25, per_page=10 → 3 pages → pages 2 and 3 fetched
        assert scraper._fetch_page_norms.call_count == 2

    @pytest.mark.asyncio
    async def test_process_documents_called_with_all_rows(self):
        scraper = _make_scraper()
        rows = [_sample_row(numeroDocumento=i) for i in range(5)]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows, total=5, per_page=10)
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(1942)

        call_docs = scraper._process_documents.call_args[0][0]
        assert len(call_docs) == 5

    @pytest.mark.asyncio
    async def test_single_request_made_when_no_pagination(self):
        """Exactly one HTTP request for a year that fits a single page."""
        scraper = _make_scraper()
        rows = [_sample_row()]
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows, total=1, per_page=10)
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(1942)

        assert scraper.request_service.make_request.call_count == 1

    @pytest.mark.asyncio
    async def test_request_has_no_especieLegislativa(self):
        """The POST body must never include especieLegislativa."""
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response([], total=0)
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(2000)

        call_kwargs = scraper.request_service.make_request.call_args[1]
        posted_json = call_kwargs.get("json", {})
        assert "especieLegislativa" not in posted_json

    @pytest.mark.asyncio
    async def test_year_injected_into_each_doc(self):
        """Every doc passed to _process_documents must have _year set."""
        scraper = _make_scraper()
        rows = [_sample_row(numeroDocumento=i) for i in range(3)]
        # Remove pre-set _year to test injection
        for r in rows:
            r.pop("_year", None)
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_api_response(rows, total=3, per_page=10)
        )
        scraper._process_documents = AsyncMock(return_value=[])

        await scraper._scrape_year(1942)

        call_docs = scraper._process_documents.call_args[0][0]
        assert all(doc.get("_year") == 1942 for doc in call_docs)


# ---------------------------------------------------------------------------
# Integration tests — live API
# ---------------------------------------------------------------------------


async def _build_live_scraper(save_dir: Path) -> AlagoasSefazScraper:
    return AlagoasSefazScraper(
        year_start=1942,
        year_end=1942,
        docs_save_dir=str(save_dir),
        overwrite=True,
        rps=5,
    )


# Mocked versions of integration tests for performance
async def test_scrape_year_1942_count_matches_api_mock():
    """Total documents returned must be within tolerance of API-reported total (mocked)."""
    scraper = _make_scraper(year_start=1942, year_end=1942)

    sample_rows = [_sample_row() for _ in range(25)]
    mock_api_resp = _make_api_response(sample_rows, total=25, per_page=25)
    long_md = "Art. 1° Conteúdo legal completo e detalhado. " * 10

    async def mock_doc_request(url, **kwargs):
        if "visualizarDocumento" in url:
            return _make_doc_response(b"mock pdf content", "decreto_001.pdf")
        return mock_api_resp

    scraper.request_service.make_request = mock_doc_request
    scraper._get_markdown = AsyncMock(return_value=long_md)
    scraper._save_doc_result = AsyncMock(side_effect=lambda doc: doc)

    await scraper._load_scraped_keys(1942)
    results = await scraper._scrape_year(1942)

    assert len(results) == 25, f"Expected 25 docs, got {len(results)}"
    await scraper.cleanup()


async def test_scrape_year_1942_type_field_from_tipoDocumento_mock():
    """Every result must have a non-empty 'type' field from tipoDocumento.descricao (mocked)."""
    scraper = _make_scraper(year_start=1942, year_end=1942)

    sample_rows = [
        _sample_row(tipoDocumento={"descricao": "Decreto"}),
        _sample_row(tipoDocumento={"descricao": "Lei Ordinária"}),
        _sample_row(tipoDocumento={"descricao": "Decreto"}),
    ]
    mock_api_resp = _make_api_response(sample_rows, total=3)
    long_md = "Art. 1° Conteúdo legal completo e detalhado. " * 10

    async def mock_doc_request(url, **kwargs):
        if "visualizarDocumento" in url:
            return _make_doc_response(b"mock pdf content", "decreto_001.pdf")
        return mock_api_resp

    scraper.request_service.make_request = mock_doc_request
    scraper._get_markdown = AsyncMock(return_value=long_md)
    scraper._save_doc_result = AsyncMock(side_effect=lambda doc: doc)

    await scraper._load_scraped_keys(1942)
    results = await scraper._scrape_year(1942)

    assert results, "No results returned"
    missing_type = [d for d in results if not d.get("type")]
    assert missing_type == [], f"{len(missing_type)} docs missing type field"

    types_found = {d["type"] for d in results}
    assert "Decreto" in types_found, "Expected Decretos"
    assert "Lei Ordinária" in types_found, "Expected Leis Ordinárias"
    await scraper.cleanup()


async def test_scrape_year_1942_no_especieLegislativa_in_requests_mock():
    """No listing request made during a full year scrape should include especieLegislativa (mocked)."""
    scraper = _make_scraper(year_start=1942, year_end=1942)

    posted_bodies: list[dict] = []

    async def tracking_make_request(url, **kwargs):
        if "json" in kwargs:
            posted_bodies.append(kwargs["json"])
        if "visualizarDocumento" in url:
            return _make_doc_response(b"mock pdf content", "decreto_001.pdf")
        return _make_api_response([_sample_row()], total=1)

    scraper.request_service.make_request = tracking_make_request
    scraper.request_service.cleanup = AsyncMock()
    scraper._get_markdown = AsyncMock(
        return_value="Art. 1° Conteúdo legal completo e detalhado. " * 10
    )
    scraper._save_doc_result = AsyncMock(side_effect=lambda doc: doc)

    await scraper._load_scraped_keys(1942)
    await scraper._scrape_year(1942)

    listing_bodies = [b for b in posted_bodies if "periodoInicial" in b]
    bad = [b for b in listing_bodies if "especieLegislativa" in b]
    assert bad == [], f"Found listing requests with especieLegislativa: {bad}"
    await scraper.cleanup()


async def test_scrape_year_1942_mixed_types_in_single_request_mock():
    """Year 1942 contains both Decreto and Lei Ordinária — both must appear (mocked)."""
    scraper = _make_scraper(year_start=1942, year_end=1942)

    sample_rows = [
        _sample_row(tipoDocumento={"descricao": "Decreto"}),
        _sample_row(tipoDocumento={"descricao": "Lei Ordinária"}),
        _sample_row(tipoDocumento={"descricao": "Decreto"}),
    ]
    mock_api_resp = _make_api_response(sample_rows, total=3)
    long_md = "Art. 1° Conteúdo legal completo e detalhado. " * 10

    async def mock_doc_request(url, **kwargs):
        if "visualizarDocumento" in url:
            return _make_doc_response(b"mock pdf content", "decreto_001.pdf")
        return mock_api_resp

    scraper.request_service.make_request = mock_doc_request
    scraper._get_markdown = AsyncMock(return_value=long_md)
    scraper._save_doc_result = AsyncMock(side_effect=lambda doc: doc)

    await scraper._load_scraped_keys(1942)
    results = await scraper._scrape_year(1942)

    types_found = {d["type"] for d in results}
    assert "Decreto" in types_found, "Expected Decretos in year 1942"
    assert "Lei Ordinária" in types_found, "Expected Leis Ordinárias in year 1942"
    await scraper.cleanup()


# ---------------------------------------------------------------------------
# Original integration tests (kept for reference but marked to skip by default)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skip(reason="Use mock versions for faster execution")
async def test_scrape_year_1942_count_matches_api():
    """Total documents returned must be within tolerance of API-reported total."""
    with tempfile.TemporaryDirectory() as tmpdir:
        scraper = await _build_live_scraper(Path(tmpdir))
        await scraper._load_scraped_keys(1942)

        # Get expected total from the API directly
        params = scraper._build_params(1942)
        resp = await scraper.request_service.make_request(
            scraper._build_url(), method="POST", json=params
        )
        assert resp, "Could not reach Alagoas SEFAZ API"
        payload = await resp.json()
        api_total = payload["registrosTotais"]
        print(f"\nAPI reports {api_total} norms for year 1942")

        results = await scraper._scrape_year(1942)
        print(f"Scraper returned {len(results)} results")

        tolerance = max(5, int(api_total * 0.05))
        assert len(results) >= api_total - tolerance, (
            f"Expected ~{api_total} docs, got {len(results)} (tolerance={tolerance})"
        )
        await scraper.cleanup()


@pytest.mark.integration
@pytest.mark.skip(reason="Use mock versions for faster execution")
async def test_scrape_year_1942_type_field_from_tipoDocumento():
    """Every result must have a non-empty 'type' field from tipoDocumento.descricao."""
    with tempfile.TemporaryDirectory() as tmpdir:
        scraper = await _build_live_scraper(Path(tmpdir))
        await scraper._load_scraped_keys(1942)
        results = await scraper._scrape_year(1942)
        assert results, "No results returned"

        missing_type = [d for d in results if not d.get("type")]
        assert missing_type == [], (
            f"{len(missing_type)} docs missing type field: "
            + ", ".join(str(d.get("id", "?")) for d in missing_type[:3])
        )
        await scraper.cleanup()


@pytest.mark.integration
@pytest.mark.skip(reason="Use mock versions for faster execution")
async def test_scrape_year_1942_no_especieLegislativa_in_requests():
    """No listing request made during a full year scrape should include especieLegislativa."""
    with tempfile.TemporaryDirectory() as tmpdir:
        scraper = await _build_live_scraper(Path(tmpdir))
        await scraper._load_scraped_keys(1942)

        posted_bodies: list[dict] = []
        original_make_request = scraper.request_service.make_request

        async def tracking_make_request(url, **kwargs):
            if "json" in kwargs:
                posted_bodies.append(kwargs["json"])
            return await original_make_request(url, **kwargs)

        scraper.request_service.make_request = tracking_make_request
        await scraper._scrape_year(1942)

        listing_bodies = [b for b in posted_bodies if "periodoInicial" in b]
        bad = [b for b in listing_bodies if "especieLegislativa" in b]
        assert bad == [], f"Found listing requests with especieLegislativa: {bad}"
        await scraper.cleanup()


@pytest.mark.integration
@pytest.mark.skip(reason="Use mock versions for faster execution")
async def test_scrape_year_1942_mixed_types_in_single_request():
    """Year 1942 contains both Decreto and Lei Ordinária — both must appear."""
    with tempfile.TemporaryDirectory() as tmpdir:
        scraper = await _build_live_scraper(Path(tmpdir))
        await scraper._load_scraped_keys(1942)
        results = await scraper._scrape_year(1942)

        types_found = {d["type"] for d in results}
        assert "Decreto" in types_found, "Expected Decretos in year 1942"
        assert "Lei Ordinária" in types_found, "Expected Leis Ordinárias in year 1942"
        await scraper.cleanup()
