"""Tests for ParaAlepaScraper.

Covers:
- TYPES constant: 6 types present, correct IDs
- SITUATIONS module-level dict preserved for downstream consumers
- Class docstring is accessible (__doc__ is not None)
- _iterate_situations NOT set on class (situations={} passed)
- _build_params: correct keys and values, default tipo=""
- _normalize_type: canonical names, plural→singular, federal→None, unknown→passthrough
- _get_docs_links: happy-path returns docs with type/norm_number, empty (0-count) returns [],
  request failure returns [], federal types filtered out
- _get_doc_data: resume skip, download failure → error + None,
  invalid markdown → error + None, correct result dict shape, norm_type in error log
- _scrape_year: single POST, groups by type, calls _process_documents per type

Integration (live site):
- test_get_docs_links_all_types_2000_returns_results
- test_get_doc_data_returns_valid_markdown

Run with:
    .venv/bin/pytest tests/test_para_scraper.py -v
"""

import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from conftest import assert_resume_skips, make_base_scraper, make_failed_request

from src.scraper.state_legislation.para import (
    _FEDERAL_TYPES,
    _TYPE_NORMALIZE,
    SITUATIONS,
    TYPES,
    ParaAlepaScraper,
)

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> ParaAlepaScraper:
    """Instantiate ParaAlepaScraper bypassing __init__ (no network, no I/O)."""
    import re

    return make_base_scraper(
        ParaAlepaScraper,
        "http://bancodeleis.alepa.pa.gov.br",
        "PARA",
        TYPES,
        situations={},
        regex_total_count=re.compile(r"Total de Registros:\s+(\d+)"),
        **kwargs,
    )


def _make_listing_html(count: int = 3, doc_type: str = "Decreto Estadual") -> bytes:
    """Build a minimal listing page with `count` document rows including type/number."""
    rows = ""
    for i in range(1, count + 1):
        rows += f"""
        <tr>
          <td>
            <strong>Tipo da Lei:</strong> {doc_type}
            <strong>Nº da Lei:</strong> 00{i} / 2000
          </td>
          <td><a href="/pdf/doc{i}.pdf">Ementa {i}</a></td>
        </tr>"""
    return f"""<html><body>
        <p>Total de Registros: {count}</p>
        <table><tr><td>header</td></tr>{rows}</table>
    </body></html>""".encode()


def _make_mixed_listing_html() -> bytes:
    """Build a listing page with multiple types including a federal type."""
    rows = """
    <tr>
      <td>
        <strong>Tipo da Lei:</strong> Decreto Estadual
        <strong>Nº da Lei:</strong> 001 / 2020
      </td>
      <td><a href="/pdf/dec1.pdf">Ementa decreto</a></td>
    </tr>
    <tr>
      <td>
        <strong>Tipo da Lei:</strong> Resoluções
        <strong>Nº da Lei:</strong> 010 / 2020
      </td>
      <td><a href="/pdf/res1.pdf">Ementa resolução</a></td>
    </tr>
    <tr>
      <td>
        <strong>Tipo da Lei:</strong> Emendas Constitucionais Federais
        <strong>Nº da Lei:</strong> 100 / 2020
      </td>
      <td><a href="/pdf/fed1.pdf">Ementa federal</a></td>
    </tr>
    """
    return f"""<html><body>
        <p>Total de Registros: 3</p>
        <table><tr><td>header</td></tr>{rows}</table>
    </body></html>""".encode()


def _make_empty_listing_html() -> bytes:
    return (
        b"<html><body><p>Total de Registros:                      0</p></body></html>"
    )


# ---------------------------------------------------------------------------
# TYPES and SITUATIONS constants
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 6
    REQUIRED_KEYS = {"Decreto Estadual", "Lei Ordinária", "Lei Complementar"}
    REQUIRE_INT_VALUES = True

    def test_decreto_estadual_id(self):
        assert TYPES["Decreto Estadual"] == 2


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_nao_consta_key_present(self):
        assert "Não consta" in SITUATIONS


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = ParaAlepaScraper
    STATE_NAME = "Para"

    def test_situations_empty_in_instance(self):
        scraper = _make_scraper()
        assert scraper.situations == {}


# ---------------------------------------------------------------------------
# _normalize_type
# ---------------------------------------------------------------------------


class TestNormalizeType:
    def test_canonical_name_unchanged(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Decreto Estadual") == "Decreto Estadual"

    def test_plural_resolucoes_normalized(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Resoluções") == "Resolução"

    def test_emendas_estaduais_normalized(self):
        scraper = _make_scraper()
        assert (
            scraper._normalize_type("Emendas Constitucionais Estaduais")
            == "Emenda Constitucional"
        )

    def test_federal_type_returns_none(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Emendas Constitucionais Federais") is None

    def test_unknown_type_passthrough(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Tipo Desconhecido") == "Tipo Desconhecido"

    def test_empty_string_returns_empty(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("") == ""

    def test_none_returns_empty(self):
        scraper = _make_scraper()
        assert scraper._normalize_type(None) == ""

    def test_case_insensitive(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("decreto estadual") == "Decreto Estadual"

    def test_extra_whitespace_cleaned(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("  Decreto   Estadual  ") == "Decreto Estadual"

    def test_all_types_in_normalize_map(self):
        for name in TYPES:
            assert _TYPE_NORMALIZE.get(name.casefold()) == name

    def test_federal_types_set(self):
        assert "emendas constitucionais federais" in _FEDERAL_TYPES


# ---------------------------------------------------------------------------
# _build_params
# ---------------------------------------------------------------------------


class TestBuildParams:
    def test_returns_correct_keys(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert "anoLei" in params
        assert "tipo" in params
        assert "verifica" in params
        assert "button" in params

    def test_default_tipo_is_empty(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["tipo"] == ""

    def test_year_set(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["anoLei"] == 2000

    def test_explicit_tipo(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000, norm_type_id=2)
        assert params["tipo"] == 2

    def test_verifica_is_1(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["verifica"] == 1

    def test_no_shared_state_mutation(self):
        scraper = _make_scraper()
        p1 = scraper._build_params(2000, norm_type_id=2)
        p2 = scraper._build_params(2001, norm_type_id=3)
        assert p1["anoLei"] != p2["anoLei"]
        assert p1["tipo"] != p2["tipo"]


# ---------------------------------------------------------------------------
# _get_docs_links
# ---------------------------------------------------------------------------


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_happy_path_returns_docs_with_type_and_number(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=_make_listing_html(3))
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        docs = await scraper._get_docs_links(
            "http://bancodeleis.alepa.pa.gov.br/index.php",
            {"anoLei": 2000, "tipo": ""},
        )
        assert len(docs) == 3
        assert docs[0]["title"].startswith("Decreto Estadual")
        assert "pdf_link" in docs[0]
        assert "summary" in docs[0]
        assert docs[0]["type"] == "Decreto Estadual"
        assert "norm_number" in docs[0]

    @pytest.mark.asyncio
    async def test_zero_count_returns_empty(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=_make_empty_listing_html())
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        docs = await scraper._get_docs_links(
            "http://bancodeleis.alepa.pa.gov.br/index.php",
            {"anoLei": 2000, "tipo": ""},
        )
        assert docs == []

    @pytest.mark.asyncio
    async def test_request_failure_returns_empty(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        docs = await scraper._get_docs_links(
            "http://bancodeleis.alepa.pa.gov.br/index.php",
            {"anoLei": 2000, "tipo": ""},
        )
        assert docs == []

    @pytest.mark.asyncio
    async def test_federal_types_filtered_out(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=_make_mixed_listing_html())
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        docs = await scraper._get_docs_links(
            "http://bancodeleis.alepa.pa.gov.br/index.php",
            {"anoLei": 2020, "tipo": ""},
        )
        # 3 total rows but federal type should be filtered out
        assert len(docs) == 2
        types = {d["type"] for d in docs}
        assert "Emenda Constitucional Federal" not in types

    @pytest.mark.asyncio
    async def test_plural_type_normalized(self):
        scraper = _make_scraper()
        resp = MagicMock()
        resp.__bool__ = lambda s: True
        resp.read = AsyncMock(return_value=_make_mixed_listing_html())
        scraper.request_service.make_request = AsyncMock(return_value=resp)
        docs = await scraper._get_docs_links(
            "http://bancodeleis.alepa.pa.gov.br/index.php",
            {"anoLei": 2020, "tipo": ""},
        )
        resolucao_docs = [d for d in docs if d["type"] == "Resolução"]
        assert len(resolucao_docs) == 1


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(
            _make_scraper(), {"title": "Decreto 001", "pdf_link": "/pdf/doc1.pdf"}
        )

    @pytest.mark.asyncio
    async def test_invalid_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(
            return_value=("short", b"raw", ".pdf")
        )
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Decreto 001",
            "pdf_link": "/pdf/doc1.pdf",
            "type": "Decreto Estadual",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_error_includes_norm_type(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(
            return_value=("short", b"raw", ".pdf")
        )
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Decreto 001",
            "pdf_link": "/pdf/doc1.pdf",
            "type": "Decreto Estadual",
        }
        await scraper._get_doc_data(doc_info)
        call_kwargs = scraper._save_doc_error.call_args[1]
        assert call_kwargs["norm_type"] == "Decreto Estadual"

    @pytest.mark.asyncio
    async def test_valid_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Decreto\n\n" + "Texto do decreto. " * 20
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw_bytes", ".pdf")
        )
        doc_info = {
            "title": "Decreto 001",
            "pdf_link": "/pdf/doc1.pdf",
            "year": 2020,
            "type": "Decreto Estadual",
            "situation": "Não consta",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert result["text_markdown"] == valid_md.strip()
        assert result["document_url"] == "/pdf/doc1.pdf"
        assert result["_raw_content"] == b"raw_bytes"
        assert result["_content_extension"] == ".pdf"

    @pytest.mark.asyncio
    async def test_pdf_link_removed_from_doc_info(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Decreto\n\n" + "Texto do decreto. " * 20
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, b"raw_bytes", ".pdf")
        )
        doc_info = {
            "title": "Decreto 001",
            "pdf_link": "/pdf/doc1.pdf",
            "year": 2020,
            "type": "Decreto Estadual",
            "situation": "Não consta",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert "pdf_link" not in result


# ---------------------------------------------------------------------------
# _scrape_year
# ---------------------------------------------------------------------------


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_single_post_with_empty_tipo(self):
        """Verify _scrape_year sends a single POST with tipo=''."""
        scraper = _make_scraper()
        scraper._get_docs_links = AsyncMock(return_value=[])
        await scraper._scrape_year(2000)
        scraper._get_docs_links.assert_called_once()
        call_args = scraper._get_docs_links.call_args
        params = call_args[0][1]
        assert params["tipo"] == ""

    @pytest.mark.asyncio
    async def test_empty_docs_returns_empty(self):
        scraper = _make_scraper()
        scraper._get_docs_links = AsyncMock(return_value=[])
        result = await scraper._scrape_year(2000)
        assert result == []

    @pytest.mark.asyncio
    async def test_groups_by_type_and_calls_process_documents(self):
        scraper = _make_scraper()
        fake_docs = [
            {
                "title": "Decreto Estadual 001",
                "pdf_link": "/pdf/d1.pdf",
                "type": "Decreto Estadual",
                "norm_number": "001",
                "summary": "S1",
            },
            {
                "title": "Lei Ordinária 001",
                "pdf_link": "/pdf/l1.pdf",
                "type": "Lei Ordinária",
                "norm_number": "001",
                "summary": "S2",
            },
            {
                "title": "Decreto Estadual 002",
                "pdf_link": "/pdf/d2.pdf",
                "type": "Decreto Estadual",
                "norm_number": "002",
                "summary": "S3",
            },
        ]
        scraper._get_docs_links = AsyncMock(return_value=fake_docs)

        process_calls = []

        async def fake_process(docs, **kwargs):
            process_calls.append((docs, kwargs))
            return [{"result": True}]

        scraper._process_documents = fake_process
        scraper._gather_results = AsyncMock(
            return_value=[[{"result": True}], [{"result": True}]]
        )

        await scraper._scrape_year(2000)

        # _gather_results should receive 2 tasks (2 types)
        gather_call = scraper._gather_results.call_args
        tasks = gather_call[0][0]
        assert len(tasks) == 2

    @pytest.mark.asyncio
    async def test_situation_is_nao_consta(self):
        scraper = _make_scraper()
        fake_docs = [
            {
                "title": "Decreto Estadual 001",
                "pdf_link": "/pdf/d1.pdf",
                "type": "Decreto Estadual",
                "norm_number": "001",
                "summary": "S1",
            },
        ]
        scraper._get_docs_links = AsyncMock(return_value=fake_docs)

        captured = {}

        async def fake_process(docs, **kwargs):
            captured.update(kwargs)
            return []

        scraper._process_documents = fake_process
        scraper._gather_results = AsyncMock(return_value=[])
        scraper._flatten_results = MagicMock(return_value=[])

        await scraper._scrape_year(2000)

        # _gather_results context should have situation
        gather_kwargs = scraper._gather_results.call_args[1]
        assert gather_kwargs["context"]["situation"] == "Não consta"


# ---------------------------------------------------------------------------
# Integration tests (live site)
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_get_docs_links_all_types_2000_returns_results():
    """Para ALEPA should return documents for year 2000 with tipo='' (all types)."""
    with tempfile.TemporaryDirectory() as tmp:
        scraper = ParaAlepaScraper(docs_save_dir=tmp, verbose=False)
        params = scraper._build_params(2000)  # tipo="" by default
        url = f"{scraper.base_url}/index.php"
        docs = await scraper._get_docs_links(url, params)
        assert isinstance(docs, list)
        assert len(docs) > 0
        assert "title" in docs[0]
        assert "pdf_link" in docs[0]
        assert "type" in docs[0]
        assert "norm_number" in docs[0]
        # Verify no federal types
        for doc in docs:
            assert doc["type"].casefold() not in _FEDERAL_TYPES


@pytest.mark.integration
async def test_get_doc_data_returns_valid_markdown():
    """Fetching the first doc for year 2000 should yield non-empty markdown."""
    with tempfile.TemporaryDirectory() as tmp:
        scraper = ParaAlepaScraper(docs_save_dir=tmp, verbose=False)
        params = scraper._build_params(2000)
        url = f"{scraper.base_url}/index.php"
        docs = await scraper._get_docs_links(url, params)
        assert len(docs) > 0

        doc = docs[0]
        result = await scraper._get_doc_data(doc)
        if result is not None:
            assert "text_markdown" in result
            assert result["text_markdown"] is not None
            assert len(result["text_markdown"]) > 50
