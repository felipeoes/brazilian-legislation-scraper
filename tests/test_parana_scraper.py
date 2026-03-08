"""Tests for ParanaCVScraper (Paraná Casa Civil).

Covers:
- TYPES constant: 7 types, integer IDs
- SITUATIONS module-level dict preserved for downstream consumers
- Class docstring accessible
- _build_form_data: correct keys and year/type values
- _search_url: correct construction with/without total_records
- _parse_results_table: returns docs, skips rows with < 4 tds,
  handles missing link_tag, handles missing table
- _infer_situation: revogado/revogada → INVALID, others → VALID
- _get_doc_data: resume skip, failed response → error + None,
  missing form → error + None, invalid markdown → None,
  valid doc returns correct shape with situation inferred

Run with:
    .venv/bin/pytest tests/test_parana_scraper.py -v
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.base.scraper import DEFAULT_INVALID_SITUATION, DEFAULT_VALID_SITUATION
from src.scraper.state_legislation.parana import SITUATIONS, TYPES, ParanaCVScraper
from base_tests import TypesConstantTests, SituationsConstantTests, ScraperClassTests
from conftest import make_base_scraper, make_failed_request, assert_resume_skips


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> ParanaCVScraper:
    """Instantiate ParanaCVScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        ParanaCVScraper,
        "https://www.legislacao.pr.gov.br",
        "PARANA",
        TYPES,
        SITUATIONS,
        _base_form_data={
            "pesquisou": "true",
            "opcaoAno": "2",
            "opcaoNro": "1",
            "optPesquisa": "tm",
            "tiposAtoStr": "",
            "site": "1",
            "codigoTipoAto": "",
            "tipoOrdenacao": "",
            "ordAsc": "false",
            "optTexto": "2",
            "texto": "",
            "anoInicialAto": "",
            "anoFinalAto": "",
            "nroInicialAto": "",
            "nroFinalAto": "",
            "tipoAto": "",
            "nroAto": "",
            "anoAto": "",
            "tema": "0",
            "anoInicialAtoTema": "",
            "anoFinalAtoTema": "",
            "nroInicialAtoTema": "",
            "nroFinalAtoTema": "",
        },
        **kwargs,
    )


def _make_results_html(count: int = 2, revogado: bool = False) -> str:
    revogado_text = " Revogado pelo Decreto 999/2010" if revogado else ""
    rows = ""
    for i in range(1, count + 1):
        rows += f"""
        <tr class="list_cor_sim">
          <td><a href="javascript:exibirAto('{100 + i}')">Lei {i}</a></td>
          <td>Lei Estadual {i}/2020{revogado_text}</td>
          <td>Dispõe sobre assunto {i}.</td>
          <td>01/01/2020</td>
        </tr>"""
    return f"""<html><body>
        <p>Página 1 de 1</p><p>Total de {count} registros</p>
        <table id="list_tabela"><tbody>{rows}</tbody></table>
    </body></html>"""


def _make_doc_html(revogado: bool = False) -> str:
    revogado_text = "Revogado pelo Decreto 999/2010" if revogado else ""
    return f"""<html><body>
        <form name="pesquisarAtoForm">
          <div>
            <p>LEI ESTADUAL N.º 1/2020</p>
            <p>Dispõe sobre assunto importante.</p>
            {revogado_text}
          </div>
        </form>
    </body></html>"""


# ---------------------------------------------------------------------------
# TYPES and SITUATIONS constants
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 7
    REQUIRED_KEYS = {"Lei", "Decreto"}
    REQUIRE_INT_VALUES = True


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = ParanaCVScraper
    STATE_NAME = "Paraná"


# ---------------------------------------------------------------------------
# _build_form_data
# ---------------------------------------------------------------------------


class TestBuildFormData:
    def test_sets_tipos_ato_str(self):
        scraper = _make_scraper()
        data = scraper._build_form_data(2020, 1)
        assert data["tiposAtoStr"] == "1"

    def test_sets_ano_inicial_ato_tema(self):
        scraper = _make_scraper()
        data = scraper._build_form_data(2020, 1)
        assert data["anoInicialAtoTema"] == "2020"

    def test_sets_ano_final_ato_tema(self):
        scraper = _make_scraper()
        data = scraper._build_form_data(2020, 1)
        assert data["anoFinalAtoTema"] == "2020"

    def test_no_mutation_of_base_form_data(self):
        scraper = _make_scraper()
        original = scraper._base_form_data.copy()
        scraper._build_form_data(2020, 1)
        assert scraper._base_form_data == original

    def test_different_years_produce_different_data(self):
        scraper = _make_scraper()
        d1 = scraper._build_form_data(2020, 1)
        d2 = scraper._build_form_data(2021, 1)
        assert d1["anoInicialAtoTema"] != d2["anoInicialAtoTema"]


# ---------------------------------------------------------------------------
# _search_url
# ---------------------------------------------------------------------------


class TestSearchUrl:
    def test_without_total_records(self):
        scraper = _make_scraper()
        url = scraper._search_url(page=1)
        assert "indice=1" in url
        assert "site=1" in url
        assert "totalRegistros" not in url

    def test_with_total_records(self):
        scraper = _make_scraper()
        url = scraper._search_url(page=2, total_records=50)
        assert "indice=2" in url
        assert "totalRegistros=50" in url

    def test_contains_base_url(self):
        scraper = _make_scraper()
        url = scraper._search_url()
        assert scraper.base_url in url


# ---------------------------------------------------------------------------
# _parse_results_table
# ---------------------------------------------------------------------------


class TestParseResultsTable:
    def test_happy_path_returns_docs(self):
        soup = BeautifulSoup(_make_results_html(2), "html.parser")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert len(docs) == 2
        assert "id" in docs[0]
        assert "title" in docs[0]
        assert "summary" in docs[0]
        assert "date" in docs[0]

    def test_missing_table_returns_empty(self):
        soup = BeautifulSoup("<html><body><p>Nothing</p></body></html>", "html.parser")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs == []

    def test_extracts_cod_ato_from_href(self):
        soup = BeautifulSoup(_make_results_html(1), "html.parser")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs[0]["id"] == "101"

    def test_skips_rows_without_link(self):
        html = """<html><body>
            <table id="list_tabela"><tbody>
              <tr class="list_cor_sim">
                <td>No link here</td>
                <td>Title</td><td>Summary</td><td>01/01/2020</td>
              </tr>
            </tbody></table>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs == []

    def test_skips_rows_with_fewer_than_4_tds(self):
        html = """<html><body>
            <table id="list_tabela"><tbody>
              <tr class="list_cor_sim">
                <td><a href="javascript:exibirAto('99')">X</a></td>
                <td>Title</td>
              </tr>
            </tbody></table>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs == []


# ---------------------------------------------------------------------------
# _infer_situation
# ---------------------------------------------------------------------------


class TestInferSituation:
    def test_revogado_pelo_returns_invalid(self):
        html = "<html><body><p>Revogado pelo Decreto 999/2010.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert ParanaCVScraper._infer_situation(soup) == DEFAULT_INVALID_SITUATION

    def test_revogada_pela_returns_invalid(self):
        html = "<html><body><p>Revogada pela Lei 888/2008.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert ParanaCVScraper._infer_situation(soup) == DEFAULT_INVALID_SITUATION

    def test_no_revogado_returns_valid(self):
        html = "<html><body><p>Dispõe sobre normas administrativas.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert ParanaCVScraper._infer_situation(soup) == DEFAULT_VALID_SITUATION

    def test_revogado_por_returns_invalid(self):
        html = "<html><body><p>Revogado por norma posterior.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert ParanaCVScraper._infer_situation(soup) == DEFAULT_INVALID_SITUATION


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(
            _make_scraper(), {"id": "101", "title": "Lei 1/2020", "date": "01/01/2020"}
        )

    @pytest.mark.asyncio
    async def test_failed_response_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        failed = make_failed_request()
        failed.status = None
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        scraper._save_doc_error = AsyncMock()
        doc = {"id": "101", "title": "Lei 1/2020", "date": "01/01/2020"}
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_form_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.status = 200
        resp.text = AsyncMock(return_value="<html><body><p>No form</p></body></html>")
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        scraper._save_doc_error = AsyncMock()
        doc = {"id": "101", "title": "Lei 1/2020", "date": "01/01/2020"}
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.status = 200
        resp.text = AsyncMock(return_value=_make_doc_html())
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        scraper._get_markdown = AsyncMock(return_value="   ")
        doc = {"id": "101", "title": "Lei 1/2020", "date": "01/01/2020"}
        result = await scraper._get_doc_data(doc)
        assert result is None

    @pytest.mark.asyncio
    async def test_valid_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.status = 200
        resp.text = AsyncMock(return_value=_make_doc_html())
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        valid_md = "# Lei Estadual 1/2020\n\n" + "Texto da lei. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = {"id": "101", "title": "Lei 1/2020", "date": "01/01/2020"}
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert "text_markdown" in result
        assert "document_url" in result
        assert result["_content_extension"] == ".html"
        assert isinstance(result["_raw_content"], bytes)

    @pytest.mark.asyncio
    async def test_valid_revogado_doc_infers_invalid_situation(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.status = 200
        resp.text = AsyncMock(return_value=_make_doc_html(revogado=True))
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        valid_md = "# Lei\n\n" + "Revogado pelo Decreto 999. " * 20
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = {"id": "101", "title": "Lei 1/2020", "date": "01/01/2020"}
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["situation"] == DEFAULT_INVALID_SITUATION

    @pytest.mark.asyncio
    async def test_valid_active_doc_infers_valid_situation(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.status = 200
        resp.text = AsyncMock(return_value=_make_doc_html(revogado=False))
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        valid_md = "# Lei\n\n" + "Dispõe sobre normas. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = {"id": "101", "title": "Lei 1/2020", "date": "01/01/2020"}
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert result["situation"] == DEFAULT_VALID_SITUATION
